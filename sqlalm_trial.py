# SQLAlchemy ORM

# Code from : https://www.youtube.com/watch?v=51RpDZKShiw

from sqlalchemy import create_engine

engine = create_engine('sqlite:///:memory:', echo=True) # in Memory, echo =True


from sqlalchemy.orm import sessionmaker

Session = sessionmaker(bind=engine)
session=Session() # Gateway to db




from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base() # t

from sqlalchemy import Column, Integer, Numeric, String

class Cookie(Base):
	__tablename__='cookies'
	
	cookie_id=Column(Integer, primary_key=True)
	cookie_name=Column(String(50), index=True)
	cookie_recipe_url=Column(String(255))
	cookie_sku=Column(String(55))
	quantity=Column(Integer())
	unit_cost=Column(Numeric(12, 2))

	def __repr__(self):
		return(f"Cookie(cookie_name={self.cookie_name}, cookie_recipe_url={self.cookie_recipe_url}, cookie_sku={self.cookie_sku}, unit_cost={self.unit_cost}, quantity={self.quantity})")



Base.metadata.create_all(engine)

cc_cookie=Cookie(cookie_name='chocolate chip',\
cookie_recipe_url='http://abc.com/cookie', \
 cookie_sku='CC01', quantity=12, \
unit_cost=0.50)

session.add(cc_cookie)
session.commit()


print(cc_cookie.cookie_id)
# Unit of work is heavy


c1=Cookie(cookie_name='peanut butter',\
cookie_recipe_url='http://abc.com/cookie1', \
 cookie_sku='PB01', quantity=12, \
unit_cost=0.25)

c2=Cookie(cookie_name='oatmeal raisin',\
cookie_recipe_url='http://abc.com/co1okie', \
 cookie_sku='EWW01', quantity=100, \
unit_cost=1.00)

session.bulk_save_objects([c1, c2]) # Primary keys are not returned 
session.commit()
print(c1.cookie_id)   # Return None

cookies = session.query(Cookie).all()
print(cookies)



print(session.query(Cookie.cookie_name, Cookie.quantity).first()) # return a tuple Query the first cookie,  for the given  query


print("\nGet Cookie quantity in ascending")
for cookie in session.query(Cookie).order_by(Cookie.quantity):
	print(f"{cookie.cookie_sku} {cookie.quantity} {cookie.cookie_name}")

# Order By descinding

from sqlalchemy import desc

print("\nGet Cookie quantity in descending")
for cookie in session.query(Cookie).order_by(desc(Cookie.quantity)):
	print(f"{cookie.cookie_sku} {cookie.quantity} {cookie.cookie_name}")


# limiting 


print("\nGet Cookie quantity in ascending and limit to 2 items")
query = session.query(Cookie).order_by(Cookie.quantity).limit(2)
print([result.cookie_name for result in query])



# database functions

from sqlalchemy import func


print("\nDatabase functions: Sum of Quantity ")
inv_count = session.query(func.sum(Cookie.quantity)).scalar() # Scalar -  None if  no result, answer to the query
print(inv_count)


# without scalar
print("\nDatabase functions: Sum of Quantity ")
inv_count = session.query(func.sum(Cookie.quantity)).first() # Scalar -  None if  no result, answer to the query
print(inv_count)

# if no action is called it will retun the generated query

print("\nDatabase functions: Count of type of cookies ")
inv_count = session.query(func.count(Cookie.quantity)).first() #  return (3, 0) 3 -  the result and 0  specificity (how accurate the result is) 
print(inv_count)



# Labeling

print("\nDatabase functions: sum of cookies ")
inv_count = session.query(func.sum(Cookie.quantity).label('inventory_count')).first() #  return (3, 0) 3 -  the result and 0  specificity (how accurate the result is) 
print(inv_count.keys())
print(inv_count)


# filtering 
print("\nFiltering by name using filter_by:")
record = session.query(Cookie).filter_by(cookie_name='chocolate chip').first()
print(record)


print("\nFiltering using filter():")
record = session.query(Cookie).filter(Cookie.cookie_name=='chocolate chip').first()
print(record)


# Causal Elements - like, in, is none, endswith, startswith, ilike
print("\nUsing like:")
query=session.query(Cookie).filter(Cookie.cookie_name.like("%chocolate%"))

for record in query: print(record.cookie_name)


# SElecting only 1 column 
print("\nSelecting a column:")
for i in session.query(Cookie.cookie_name): print(i)


# Operators

print("\nOperator")
from sqlalchemy import cast
query = session.query(Cookie.cookie_name, cast((Cookie.quantity * Cookie.unit_cost), Numeric(12, 2)).label('inv_cost'))

for result in query:
	print(f"{result.cookie_name} - {result.inv_cost}")

# Expunge  -  

# Conjunctions

from sqlalchemy import and_, or_, not_

print('\nOR Operation:')
query =  session.query(Cookie).filter(or_(Cookie.quantity.between(10, 30), Cookie.cookie_name.contains('chip')))


for result in query: print(result.cookie_name)


# updating cookies


query = session.query(Cookie)

cc_cookies = query.filter(Cookie.cookie_name=='chocolate chip').first()

cc_cookie.quantity = cc_cookie.quantity + 120

session.commit()

print('\nUpdating cookies to new quantity:')
print(cc_cookie.quantity)


# Deleting Elements


query = session.query(Cookie)

query =  query.filter(Cookie.cookie_name=='peanut butter')

dcc_cookie = query.one()  # Generative  query, one checks if there is only one record only then it retuns a success

session.delete(dcc_cookie) # No Undo 

session.commit()

dcc_cookie = query.first()
print('\nDeleting pbutter cookie')
print(dcc_cookie)


# Relationships

from datetime import datetime
from sqlalchemy import DateTime, ForeignKey, Boolean
from sqlalchemy.orm import relationship, backref


class User(Base):
	__tablename__ = 'users'
	
	user_id = Column(Integer(), primary_key=True)
	username = Column(String(15), nullable=False, unique=True)
	email_address = Column(String(255), nullable=False)
	phone = Column(String(20), nullable=False)
	password = Column(String(25), nullable=False)
	created_on = Column(DateTime(), default=datetime.now)
	updated_on = Column(DateTime(), default=datetime.now, onupdate=datetime.now)


class Order(Base):
	__tablename__ = 'orders'
	
	order_id = Column(Integer(), primary_key=True)
	user_id = Column(Integer(), ForeignKey('users.user_id')) # allows to refers on time
	shipped = Column(Boolean(), default=False)

	user = relationship('User', backref=backref('orders'))
	

class LineItem(Base):
	__tablename__='line_items'
	
	line_item_id = Column(Integer(), primary_key=True)
	order_id = Column(Integer(), ForeignKey('orders.order_id'))
	cookie_id = Column(Integer(), ForeignKey('cookies.cookie_id'))
	quantity = Column(Integer())
	extended_cost = Column(Numeric(12, 2))

	order = relationship("Order", backref=backref("line_items"))
	cookie = relationship("Cookie", uselist=False)


# Persist the new realtionship table 
Base.metadata.create_all(engine)


cookiemon = User(username='cookiemon', email_address='mon@cookie.com', \
phone='111-111-1111', password='password')

session.add(cookiemon)

session.commit()


# Create an order
o1 = Order()


# add  a user to the order
o1.user = cookiemon
session.add(o1)

# Add a line item to the order
cc = session.query(Cookie).filter(Cookie.cookie_name=='chocolate chip').one()

line1 = LineItem(cookie=cc, quantity=2, extended_cost=1.00)

pb = session.query(Cookie).filter(Cookie.cookie_name=='oatmeal raisin').one()

line2 = LineItem(quantity = 12, extended_cost=2.00)
line2.cookie=pb


# Add line items to the order
o1.line_items.append(line1)
o1.line_items.append(line2)

session.commit()

# using relationship in a query 
query = session.query(Order.order_id, User.username, User.phone, Cookie.cookie_name, LineItem.quantity, LineItem.extended_cost)

query.join(User).join(LineItem).join(Cookie)
results = query.filter(User.username == 'cookiemon').all()

print('\nPrinting order for cookiemon user ')
print(results)


query = session.query(User.username, func.count(Order.order_id))
query = query.outerjoin(Order).group_by(User.username)

print('\nPrinting orders by a user')
for row in query: print(row)


# Automaps 
# Geospacial Queries 



# echo = True,to understand execution, 
# profiler
# can exist anywhere 
# Core ORM
