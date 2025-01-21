import psycopg2 as psycopg2
from config import config


#EX1_Connect to the PostgreSQL database server
def connect():
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = 'SELECT *FROM person;'
        cursor.execute(SQL)
        row = cursor.fetchone()
        print(row)
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()


#EX1_1 Query data from the person table
def select1():
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = 'SELECT *FROM person;'
        cursor.execute(SQL)
        row = cursor.fetchall()
        print(row)
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()


#EX1_2 Query the column names of a table
def select2():
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = 'SELECT *FROM person;'
        cursor.execute(SQL)
        colnames = [desc[0] for desc in cursor.description]
        print(colnames)
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()


#EX1_3 Query for the certificate table column names and rows
def select3():
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = 'SELECT *FROM certificates;'
        cursor.execute(SQL)
        colnames = [desc[0] for desc in cursor.description]
        print(f"column names: {colnames}")
        row = cursor.fetchall()
        print(f"rows: {row}")
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()


#EX1_4 Query for person table average age
def select4():
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = 'SELECT AVG(age) FROM person;'
        cursor.execute(SQL)
        avg_age = cursor.fetchall()
        print(f"Average age: {avg_age}")
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

#EX1_5 Query for person names of people with AZURE certificate
def select5():
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = "SELECT p.name FROM person p JOIN certificates c ON p.id = c.person_id WHERE c.name = 'Azure';"
        cursor.execute(SQL)
        rows = cursor.fetchall()
        print(rows)
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()


#EX2_1 Insert a new row into certificates table
def insert1(person_id = int, certname = str):
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = "INSERT INTO certificates (person_id, name) VALUES (%s, %s);"
        data = (person_id, certname)
        cursor.execute(SQL,data)
        con.commit()
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

#EX2_2 Add a new person to the person table by entering values after the function call
def insert2(name = str, age = int, student = bool):
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = "INSERT INTO person (name, age, student) VALUES (%s, %s, %s);"
        data = (name, age,student)
        cursor.execute(SQL,data)
        con.commit()
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

#EX3_1 Update the age of a person in the person table
def update1(name = str, age = int):
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = "UPDATE person SET age = %s WHERE name = %s;"
        data = (age, name)
        cursor.execute(SQL,data)
        con.commit()
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

#EX3_2 Update the name of a certificate in the certificates table
def update2(newname = str, id = int):
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = "UPDATE certificates SET name = %s WHERE id = %s;"
        data = (newname, id)
        cursor.execute(SQL,data)
        con.commit()
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

#EX4_1 Delete a person from the person table
def delete1(id = int):
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = "DELETE FROM person WHERE id = %s;"
        data = (id,)
        cursor.execute(SQL,data)
        con.commit()
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

#EX4_2 Delete a certificate from the certificates table
def delete2(id = int):
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = "DELETE FROM certificates WHERE id = %s;"
        data = (id,)
        cursor.execute(SQL,data)
        con.commit()
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

#EX5_1 Create a new table
def create_table():
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = "CREATE TABLE transactions (id SERIAL PRIMARY KEY, person_id INT, account_balance INT, CONSTRAINT fk_person FOREIGN KEY (person_id) REFERENCES person(id) ON DELETE CASCADE);"
        cursor.execute(SQL)
        con.commit()
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

def insertransaction(person_id = int, account_balance = int):
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = "INSERT INTO transactions (person_id, account_balance) VALUES (%s, %s);"
        data = (person_id, account_balance)
        cursor.execute(SQL,data)
        con.commit()
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

def selecttransaction():
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = 'SELECT *FROM transactions;'
        cursor.execute(SQL)
        row = cursor.fetchall()
        print(row)
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

# Making a transaction
def maketransaction(account_from = int, account_to = int, amount = int):
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        cursor.execute("SELECT COUNT(*) FROM transactions WHERE person_id = %s", (account_from,))
        if cursor.fetchone()[0] == 0:
            raise ValueError(f"Person_id {account_from} does not exist")
        
        cursor.execute("SELECT COUNT(*) FROM transactions WHERE person_id = %s", (account_from,))
        if cursor.fetchone()[0] == 0:
            raise ValueError(f"Person_id {account_to} does not exist")
        
        SQL1 = "UPDATE transactions SET account_balance= account_balance - %s WHERE person_id = %s;"
        data1 = (amount, account_from)
        cursor.execute(SQL1,data1)
        
        SQL2 = "UPDATE transactions SET account_balance = account_balance + %s WHERE person_id = %s;"
        data2 = (amount, account_to)
        cursor.execute(SQL2,data2)
        
        SQL3 = "SELECT account_balance FROM transactions WHERE person_id = %s;"
        data3 = (account_from,)
        cursor.execute(SQL3,data3)
        balance = cursor.fetchone()[0]
        if balance < 0:
            raise ValueError("Insufficient funds")

        con.commit()
        print(f"Money transferred: {amount} from account {account_from} to account {account_to}")
        cursor.close()
    except (Exception, psycopg2.Error) as error:
        print(f"{error}, rolling back transaction")
        con.rollback()
    finally:
        if con is not None:
            con.close()

# Making a command prompt application
def application():
    try:
        while True:
            selection = int(input("1) SELECT\n2) INSERT\n3) UPDATE\n4) DELETE\n5) MAKE TRANSACTION\n6) EXIT\n"))
            if selection == 6:
                print("Exiting application")
                break
            elif selection == 1:
                print("Which table do you want to select from?")
                table = int(input("1) SELECT person\n2) SELECT certificates\n3) SELECT transactions\n"))
                if table == 1:
                    select1()
                elif table == 2:
                    select3()
                elif table == 3:
                    selecttransaction()
            elif selection == 2:
                print("Which table do you want to insert into?")
                table = int(input("1) INSERT person\n2) INSERT certificates\n3) INSERT transactions\n"))
                if table == 1:
                    name = input("Enter name: ")
                    age = int(input("Enter age: "))
                    student = bool(input("Is he/she a student? (True/False) "))
                    insert2(name, age, student)
                elif table == 2:
                    person_id = int(input("Enter person_id: "))
                    certname = input("Enter certificate name: ")
                    insert1(person_id, certname)
                elif table == 3:
                    person_id = int(input("Enter person_id: "))
                    account_balance = int(input("Enter account balance: "))
                    insertransaction(person_id, account_balance)
            elif selection == 3:
                print("Which table do you want to update?")
                table = int(input("1) UPDATE persons age\n2) UPDATE certificate name\n"))
                if table == 1:
                    name = input("Enter name: ")
                    age = int(input("Enter new age: "))
                    update1(name, age)
                elif table == 2:
                    id = int(input("Enter certificate id: "))
                    newname = input("Enter new certificate name: ")
                    update2(newname, id)
            elif selection == 4:
                print("Which table do you want to delete from?")
                table = int(input("1) DELETE person\n2) DELETE certificate\n"))
                if table == 1:
                    id = int(input("Enter person id: "))
                    delete1(id)
                elif table == 2:
                    id = int(input("Enter certificate id: "))
                    delete2(id)
            elif selection == 5:
                account_from = int(input("Enter account from (person_id): "))
                account_to = int(input("Enter account to (person_id): "))
                amount = int(input("Enter amount: "))
                maketransaction(account_from, account_to, amount)
            else:
                print("Invalid selection")
    except ValueError as error:
        print("Please give a number")


def createimagetable():
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = "CREATE TABLE images (id SERIAL PRIMARY KEY, name VARCHAR(255), person_id INT, image BYTEA, CONSTRAINT fk_person FOREIGN KEY (person_id) REFERENCES person(id) ON DELETE CASCADE);"
        cursor.execute(SQL)
        con.commit()
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

def insertimage():
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = "INSERT INTO images (name, person_id, image) VALUES (%s, %s, %s);"
        with open('src\\data\\tonninseteli.jpg', 'rb') as f:
            binary_data = f.read()
        data = ('image1', 6, binary_data)
        cursor.execute(SQL,data)
        con.commit()
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

def getimage(image_id = int, output_path = str):
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = "SELECT image FROM images WHERE id = %s;"
        data = (image_id,)
        cursor.execute(SQL,data)
        image = cursor.fetchone()[0]
        with open(output_path, 'wb') as f:
            f.write(image)
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()


if __name__ == '__main__':
    getimage(1, 'src\\data\\tonninseteli2.jpg')