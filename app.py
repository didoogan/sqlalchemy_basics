from sqlalchemy import text
from sqlalchemy import create_engine


engine = create_engine("sqlite://", echo=True)
# engine = create_engine("sqlite:///test.db", echo=True)

connection = engine.connect()

query_text = "select 'hello world' as greeting"
query = text(query_text)
result = connection.execute(query)

row = result.first()

print(row.greeting)
print(row._mapping)

connection.close()

with engine.connect() as conn:
    print(conn.execute(text(query_text)).all())

################# ORM models
    
from sqlalchemy import ForeignKey, func, insert, select
from sqlalchemy.orm import DeclarativeBase, MappedAsDataclass, Mapped, mapped_column
from datetime import datetime
from typing import Optional

class Base(MappedAsDataclass, DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "user_account"

    id: Mapped[int] = mapped_column(primary_key=True, init=False)
    name: Mapped[str]
    fullname: Mapped[Optional[str]]
    # created_at: Mapped[datetime] = mapped_column(default_factory=datetime.utcnow)
    created_at: Mapped[datetime] = mapped_column(init=False, server_default=func.now())


class Address(Base):
    __tablename__ = "address"

    id: Mapped[int] = mapped_column(primary_key=True)
    email_address: Mapped[str]
    user_id: Mapped[int] = mapped_column(ForeignKey("user_account.id"))



with engine.begin() as conn:
    Base.metadata.create_all(conn)

user = User("Dima", "didoogan")

########## Inserts
query = insert(User)
print(query)

query = insert(User).values(name="user1", fullname="first user fullname")
print(query)

    # To see value nedd add parameter 
query = insert(User).values(name="user1", fullname="first user fullname").compile(compile_kwargs={"literal_binds": True})
print(query)

    # commit to db
with engine.begin() as conn:
    conn.execute(query)

    # Insert bulk values using core
with engine.begin() as conn:
    conn.execute(
        insert(User),
        [
            {"name": "user2", "fullname": "user2 fullname"},
            {"name": "sandy", "fullname": "Sandy Cheeks"},
            {"name": "gary", "fullname": "Gary the Snail"},
        ],
    )

    conn.execute(
        insert(Address),
        [
            {"email_address": "user1@gamil.com", "user_id": 1},
            {"email_address": "user2@gamil.com", "user_id": 2},
            {"email_address": "sandy@gmail.com", "user_id": 3},
            {"email_address": "sandy_spare@gmail.com", "user_id": 3},
            {"email_address": "gary@gamil.com", "user_id": 4},
            {"email_address": "gary_spare@gamil.com", "user_id": 4},
        ],
    )

# Select data

    # using raw text()
query = text("SELECT name FROM user_account")
with engine.connect() as conn:
    for row in conn.execute(query):
        print(row.name)

    # using select()
query = select(User.name, User.id) # or use `select (User)` for selecting all fields (*)
with engine.connect() as conn:
    for row in conn.execute(query):
        print(row.name, row.id)

    # select() with join()
query = select(User.name, User.fullname, Address.email_address).join_from(User, Address)
with engine.connect() as conn:
    result = conn.execute(query)
    for row in result:
        print(f"{row.name:15} {row.fullname:25}  {row.email_address}")

    # subquery()
email_address_count = (
    select(Address.user_id, func.count(Address.email_address).label("email_count")).group_by(Address.user_id).
    having(func.count(Address.email_address) > 1).subquery()
)

query = select(User.name, email_address_count.c.email_count).join_from(User, email_address_count)
with engine.connect() as conn:
    for row in conn.execute(query):
        print(f"username: {row.name} | number of email address: {row.email_count}")

### ORM
from sqlalchemy.orm import sessionmaker


session_factory = sessionmaker(bind=engine)

with session_factory() as sess:
    print(sess.execute(select(User.id)).all())

   # In the most cases we should manage session inside context manager
session_that_we_will_keep_pened_only_for_the_purpose_of_example = session_factory()
session = session_that_we_will_keep_pened_only_for_the_purpose_of_example

user3 = User("user3", "user3 fullname")

session.add(user3) # It's not in db yet its in pending status

print(session.new) # Here we could see what would be commited

    # to persist user3 we should call session.commit() but it will be commited implisatly if we will make select

query = select(User).where(User.name == "user3")
result = session.execute(query)
row = result.first()
also_user3 = row[0]

    # user3 and also_user3 are equal objects
print(id(also_user3))
print(id(user3))
    # they behave so because of identity_map cache python object:
print(dict(session.identity_map))

    # when working with sessions its more convinient to use scalars() method
this_too_user3 = session.scalars(select(User).where(User.name == "user3")).first()
print(this_too_user3)

    # bulk insert
session.add_all(
    [
        User("user4", "user4 fullname"),
        User("user5", "user5 fullname"),
    ]
)