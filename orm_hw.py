import os
import json
import sqlalchemy
import sqlalchemy as sq
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

Base = declarative_base()

class Publisher(Base):
    __tablename__ = "publisher"

    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String(length=40), unique=True)

class Book(Base):
    __tablename__= "book"

    id = sq.Column(sq.Integer, primary_key=True)
    title = sq.Column(sq.String(length=60))
    id_publisher = sq.Column(sq.Integer, sq.ForeignKey("publisher.id"), nullable=False)

    publisher = relationship(Publisher, backref="book")

class Shop(Base):
    __tablename__ = "shop"

    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String(length=60), unique=True)

class Stock(Base):
    __tablename__ = "stock"

    id = sq.Column(sq.Integer, primary_key=True)
    id_book = sq.Column(sq.Integer, sq.ForeignKey("book.id"), nullable=False)
    id_shop = sq.Column(sq.Integer, sq.ForeignKey("shop.id"), nullable=False)
    count = sq.Column(sq.Integer)

    book = relationship(Book, backref="stock")
    shop = relationship(Shop, backref="stock")

class Sale(Base):
    __tablename__ = "sale"

    id = sq.Column(sq.Integer, primary_key=True)
    price = sq.Column(sq.Float)
    date_sale = sq.Column(sq.Date)
    id_stock = sq.Column(sq.Integer,sq.ForeignKey("stock.id"),nullable=False)
    count = sq.Column(sq.Integer)

    stock = relationship(Stock, backref="sale")

def create_tables(engine):
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


conn_driver = 'postgresql'
login = os.getenv('DB_USER', 'postgres')
password = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME', 'Books_sales_db')
server_name = os.getenv('DB_HOST', 'localhost')
port = os.getenv('DB_PORT', '5432')

DSN = f"{conn_driver}://{login}:{password}@{server_name}:{port}/{db_name}"
engine = sqlalchemy.create_engine(DSN)
create_tables(engine)

#сессия
Session = sessionmaker(bind=engine)
session = Session()

with open('tests_data.json', 'r') as fd:
    data = json.load(fd)

for record in data:
    model = {
        'publisher': Publisher,
        'shop': Shop,
        'book': Book,
        'stock': Stock,
        'sale': Sale, 
    }[record.get('model')]
    session.add(model(id=record.get('pk'), **record.get('fields')))
session.commit()



publisher_input = input('Enter publisher name or id:  ')

if publisher_input.isdigit():
    # Если это целое число, считаем, что это идентификатор
    publisher_input = int(publisher_input)
    results = session.query(
        Book.title.label('book_title'),
        Shop.name.label('shop_name'),
        Sale.price,
        Sale.date_sale
    ).join(Publisher, Publisher.id == Book.id_publisher)\
    .join(Stock, Stock.id_book == Book.id) \
    .join(Sale, Sale.id_stock == Stock.id) \
    .join(Shop, Shop.id == Stock.id_shop) \
    .filter(Publisher.id == publisher_input) \
    .all()
    
else:
    # Если это строка, считаем, что это имя издателя
    results = session.query(
        Book.title.label('book_title'),
        Shop.name.label('shop_name'),
        Sale.price,
        Sale.date_sale
    ).join(Publisher, Publisher.id == Book.id_publisher)\
    .join(Stock, Stock.id_book == Book.id) \
    .join(Sale, Sale.id_stock == Stock.id) \
    .join(Shop, Shop.id == Stock.id_shop) \
    .filter(Publisher.name == publisher_input) \
    .all()


# Вывод результатов
for result in results:
    print(result.book_title, result.shop_name, result.price, result.date_sale)

# Закрытие сессии
session.close()
