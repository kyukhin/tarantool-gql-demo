box.cfg{}

box.schema.sequence.create('products_seq', {})
products = box.schema.space.create('Products')
products:create_index('pk', {sequence='products_seq'})
products:format{
    {name='id', type='unsigned'},
    {name='name', type='string'},
    {name='price', type='number'}}

box.schema.sequence.create('employees_seq', {})
employees = box.schema.space.create('Employees')
employees:create_index('pk', {sequence='employees_seq'})
employees:format{
    {name='id', type='unsigned'},
    {name='name', type='string'},
    {name='title', type='string'},
    {name='email', type='string'}}

box.schema.sequence.create('customers_seq', {})
customers = box.schema.space.create('Customers')
customers:create_index('pk', {sequence='customers_seq'})
customers:format{
    {name='id', type='unsigned'},
    {name='company_name', type='string'},
    {name='address', type='string'}}

box.schema.sequence.create('orders_seq', {})
orders = box.schema.space.create('Orders')
orders:create_index('pk', {sequence='orders_seq'})
orders:create_index('sk', {parts={2, 'unsigned'}, unique=false})
orders:format{
    {name='id', type='unsigned'},
    {name='customer_id', type='unsigned'},
    {name='employee_id', type='unsigned'},
    {name='shipping_method', type='string'},
    {name='address', type='string'},
    {name='freight_charge', type='number'}}

box.schema.sequence.create('order_details_seq', {})
odetails = box.schema.space.create('Order_Details')
odetails:create_index('pk', {sequence='order_details_seq'})
odetails:create_index('sk', {parts={2, 'unsigned'}, unique=false})
odetails:format{
    {name='id', type='unsigned'},
    {name='order_id', type='unsigned'},
    {name='product_id', type='unsigned'},
    {name='quantity', type='unsigned'}}

local db = {}
function add_product(name, price)
    return products:insert{nil, name, price}
end
db.add_product = add_product
db.product = function(product_id)
    return products:select{product_id}[1]
end

function add_employee(name, title, email)
    return employees:insert{nil, name, title, email}
end
db.add_employee = add_employee

db.employee = function(emp_id)
    return employees:get{emp_id}
end

function add_customer(name, address)
    return customers:insert{nil, name, address}
end
db.add_customer = add_customer
db.customer = function(id)
    row = customers:get{id}
    print(row['name'])
    return {id = row['id'], name = row['company_name'], address = row['address']}
end

function add_order(customer_id, employee_id, sm, address, fcharge)
    return orders:insert{nil, customer_id, employee_id, sm, address, fcharge}
end
db.add_order = add_order
db.order = function(order_id)
    return orders:get{order_id}
end

db.order_items = function(order_id)
    return odetails.index.sk:select{order_id}
end

db.find_orders_by_cust_id = function(cust_id)
    return orders.index.sk:select{cust_id}
end

function add_order_item(order_id, product_id, qty)
    return odetails:insert{nil, order_id, product_id, qty}
end
db.add_order_item = add_order_item

return db
