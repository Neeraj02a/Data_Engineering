def fibonacchi_fun(num, x,y):
    if(num <= x):
        print("above is your recursive fibonachi series by using recursive function :")
    else:
        next_num = x+y
        x=y
        y= next_num
        print(next_num)
        return(fibonacchi_fun(num,x,y))

if __name__=='__main__':

    print("Printing fibonacci series :")
    num = int(input("Enter the number till witch you want to print the series :"))
    a=0
    b=1

    print(a)
    print(b)

    for var in range(num):
        c = a+b
        a = b
        b = c
        print(c)

#========================================================= fibonacci series using recursive function =========================================================================
    print("========================================================= fibonacci series using recursive function ==================================================================")
    print(0)
    print(1)
    fibonacchi_fun(num,0,1)
