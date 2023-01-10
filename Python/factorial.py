if __name__=='__main__':

    num = int(input("Input any number who's factorial you want to check:"))
    
    for var in range(1,num):
        if num % var ==0:
            print(var)

