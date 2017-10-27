local parse = require 'graphql.parse'
local schema = require 'graphql.schema'
local types = require 'graphql.types'
local validate = require 'graphql.validate'
local execute = require 'graphql.execute'

local db = require('ddl')
local json = require('json')

local ast = parse [[
query orders_by_cust($cust_id: ID!) {
  customer(cust_id: $cust_id) {
    customer_name: name
  }
  orders_by_cust(cust_id: $cust_id) {
      order_id: id
      shipping_method: sm, 
      employee: emp {
        name
      },
      items {
        product {
         name,
         price
        },
        qty
      },
      freight_charge: charge,
      total_price
  }
}

mutation new_product($name: String!, $price: Float!) {
  new_product(name: $name, price: $price) {
    id
  }
}

mutation new_employee($name: String!, $title: String!, $email: String!) {
  new_employee(name: $name, title: $title, email: $email) {
    id
  }
}

mutation new_customer($name: String!, $address: String!) {
  new_customer(name: $name, address: $address) {
    id
  }
}

mutation new_order($cust_id: ID!, $emp_id: ID!, $sm: String!, $addr: String!, $charge: Float,
                   $items: [inpOrderItem]) {
  new_order(cust_id: $cust_id, emp_id: $emp_id, sm: $sm, addr: $addr, charge: $charge, items: $items) {
    id
  }
}
]]

local Product = types.object {
    name = 'Product',
    fields = {
        id = types.id.nonNull,
        name = types.string.nonNull,
        price = types.float.nonNull
    }
}

local Employee = types.object {
    name = 'Employee',
    fields = {
        id = types.id.nonNull,
        name = types.string.nonNull,
        title = types.string.nonNull,
        email = types.string.nonNull
    }
}

local inpOrderItem = types.inputObject {
    name = 'inpOrderItem',
    fields = {
        product = types.id.nonNull,
        qty = types.int.nonNull
    }
}

local Customer = types.object {
    name = 'Customer',
    fields = {
        id = types.id.nonNull,
        name = types.string.nonNull,
        address = types.string.nonNull
    }
}

local OrderItem = types.object {
    name = 'OrderItem',
    fields = {
        product = types.nonNull(Product),
        qty = types.int.nonNull
    }
}

local Order = types.object {
    name = 'Order',
    fields = {
        id = types.id.nonNull,
        cust_id = types.id,  -- .nonNull,
        emp = Employee, -- types.nonNull(Employee),
        sm = types.string.nonNull,
        addr = types.string,
        charge = types.float,
        items = types.list(OrderItem),
        total_price = types.float.nonNull
    }
}

function resolveEmployee(emp_id)
    local t = db.employee(emp_id)
    return {id = t['id'], name = t['name'], title = t['title'], email = t['email']}
end

function resolveProduct(prod_id)
    local t = db.product(prod_id)
    return  {id = t['id'], name = t['name'], price = t['price']}
end

function resolveOrderItems(order_id)
    local raw_items = db.order_items(order_id)
    local res = {}
    for _, v in pairs(raw_items) do
        table.insert(res, {product = resolveProduct(v['product_id']), qty = v['quantity']})       
    end
    return res
end

function resolveOrderTotalPrice(items)
    local res = 0
    for _, v in pairs(items) do
        res = res + v.qty * v.product.price;
    end

    return res
end

function resolveOrder(o)
    local items = resolveOrderItems(o['id'])
    return {id = o['id'], cust_id = o['customer_id'],
            emp = resolveEmployee(o['employee_id']),
            sm = o['shipping_method'], addr = o['address'],
            charge = o['freight_charge'],
            items = items,
            total_price = resolveOrderTotalPrice(items)}
end

local schema = schema.create {
    query = types.object {
        name = 'Query',
        fields = {
            orders_by_cust = {
                kind = types.list(Order),
                arguments = {
                    cust_id = types.id.nonNull
                },
                resolve = function(rootValue, args)
                    local order_idx = db.find_orders_by_cust_id(args.cust_id)
                    local orders = {}
                    for _, o in pairs(order_idx) do
                        table.insert(orders, resolveOrder(o))
                    end

                    return orders
                end
            },
            customer = {
                kind = Customer,
                arguments = {
                    cust_id = types.id.nonNull
                },
                resolve = function(rootValue, args)
                    local res = db.customer(args.cust_id)
                    print(args.cust_id)
                    return res
                end
            }
        }
    },
    mutation = types.object {
        name = 'Mutation',
        fields = {
            new_product = {
                kind = Product,
                arguments = {
                    name = types.string.nonNull,
                    price = types.float.nonNull
                },
                resolve = function(rootValue, args)
                    local p = db.add_product(args.name, args.price)
                    return {
                        id = p[1]
                    }
                end
            },

            new_employee = {
                kind = Employee,
                arguments = {
                    name = types.string.nonNull,
                    title = types.string.nonNull,
                    email = types.string.nonNull
                },
                resolve = function(rootValue, args)
                    local e = db.add_employee(args.name, args.title, args.email)
                    return {
                        id = e[1]
                    }
                end
            },

            new_customer = {
                kind = Customer,
                arguments = {
                    name = types.string.nonNull,
                    address = types.string.nonNull
                },
                resolve = function(rootValue, args)
                    local c = db.add_customer(args.name, args.address)
                    return {
                        id = c[1]
                    }
                end
            },

            new_order = {
                kind = Order,
                arguments = {
                    cust_id = types.id.nonNull,
                    emp_id = types.id.nonNull,
                    sm = types.string.nonNull,
                    addr = types.string,
                    charge = types.float,
                    items = types.list(inpOrderItem)
                },
                resolve = function(rootValue, args)
                    local o = db.add_order(tonumber(args.cust_id), tonumber(args.emp_id), args.sm,
                                           args.addr, args.charge)
                    for _, v in pairs(args.items) do
                        db.add_order_item(o[1], tonumber(v.prod_id), tonumber(v.qty))
                    end

                    return {
                        id = o[1]
                    }
                end
            }
        }
    }
}

validate(schema, ast)

local products = {}
local employess = {}
local customers = {}

for i=1,100 do
    table.insert(products, execute(schema, ast, {}, {name = 'product-'..i, price=i}, 'new_product')['new_product']['id'])
end

for i=1,20 do
    table.insert(employees, execute(schema, ast, {}, {name = 'emp1', title='sales1', email='e@mail'}, 'new_employee')['new_employee']['id'])
end

for i=1,100 do
    table.insert(customers, execute(schema, ast, {}, {name = 'cust1', address='CA, 90210'}, 'new_customer')['new_customer']['id'])
end
for i=1,1000 do
    local pos = {}
    for j=1,math.random(100) do
        table.insert(pos, {prod_id=products[math.random(#products)], qty = math.random(100)})
    end
    execute(schema, ast, {}, {cust_id = customers[math.random(#customers)],
                              emp_id = employees[math.random(#employees)],
                              sm = 'parAvion',
                              addr = 'dummy',
                              charge = math.random(500),
                              items = pos}, 'new_order')
end

print(json.encode(execute(schema, ast, {}, {cust_id=math.random(100)}, 'orders_by_cust')))
