if __name__=='__main__':

    num = int(input('Enter number you want to check : '))
    count = 0
    fact = 1
    for x in range(1, num):
        if num%x==0:
            count = count + 1

    if count > 1:
        print("-- Enter number is not prime number --")

    else:
        print("-- Enter number is a prime number --")
        print("Factorial of the prime number : ")
        for var in range(1,num+1):
            if num%var==0:
                print(var)




