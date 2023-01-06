# simple class program

if __name__=='__main__':

    class Dog:

        # class attribute
        attr = "mammal"

        # Instance attribute
        def __init__(self,name):
            self.name = name

        # Driver code
        # Object instantiation

    Rodger = Dog("Rodger")
    Tommy = Dog("Tommy")

    # Accessing class attributes
    print("Rodger is a {}".format(Rodger.__class__.attr))
    print("Tommy is a {}".format(Tommy.__class__.attr))

    # Accessing instance attribute
    print("My name is {}".format(Rodger.name))
    print("My name is {}".format(Tommy.name))

#=============================================================================================================================================================================
print("\n==================================================================== project 2 ==================================================================================\n")
class Dog2:

    # class attribute
    attr1 = "mammal"

    # Instance attribute
    def __init__(self, name):
        self.name =name

    def speak(self):
        print("My name is", self.name)


# Driver code
# Object instantiation

Rodger = Dog2("Rodger")
Tommy = Dog2("Tommy")

# Accessing class method
Rodger.speak()
Tommy.speak()

#================================================================== Inheritance =============================================================================================
print("\n========================================================================= Inheritance ============================================================================\n")
class Person(object):

    #__init__ is known as the constructor
    def __init__(self,name,idnumber):
        self.name = name
        self.idnumber = idnumber

    # invoking the __init__ of the parent class
    #person.__init__(self, name, idnumber)

    def details(self):
        print("myname is:", self.name)
        print("IdNumber :", self.idnumber)

# child class

class Employee(Person):

    def __init__(self, name, idnumber, salary,post):
        self.salary= salary
        self.post = post

    # invoking the __init__ of the parents class
        Person.__init__(self, name, idnumber)

    def details(self):
        print("myname is :", self.name)
        print("IdNumber :", self.idnumber)
        print("Post :", self.post)

# creation of an object variable or an instance
a=Employee("Rahul",886012,200000,"Intern")

# Calling a function of the class person using its instance
a.details()
a.details()


#==================================================================== Polymorphism =========================================================================================
class Bird:
    def into(self):
        print("There are many types of birds :")

    def flight(self):
        print("most of the birds can flay but some can not")

class Sparrow(Bird):
    def flight(self):
        print("Sparrow can fly")

class Ostrich(Bird):
    def flight(self):
        print("Ostriches can not fly")

obj_bird = Bird()
obj_spr = Sparrow()
obj_ost = Ostrich()

obj_bird.into()
obj_bird.flight()

obj_spr.into()
obj_spr.flight()

obj_ost.into()
obj_ost.flight()

# ================================================================== Encapulation ========================================================================================
print("\n================================================================== Encapulation ====================================================================================\n")
# python program to demonstrate private members

# creating a Base Class
class Base:
    def __init__(self):
        self.a="Geeksfor Geeks"
        self.__c="Geeksfor Geeks"
# Creating a derived class
class Derived(Base):
    def __init__(self):

        # calling constructor of base class
        Base.__init__(self)
        print("calling private member of base class:")
        print(self.__c)



#Driver call
obj1=Base()
print(obj1.a)

#=================================================================== Data Abstraction =====================================================================================
print("\n============================================================ Data Abstraction =====================================================================================\n")
class Employee():
    def emp_id(self, id, name,age,salary):
        pass
        #Abstraction

class Childemployee1(Employee):
    def emp_id(self, id):
        print("emp_id is 12345")

emp1 = Childemployee1()
emp1.emp_id(id)