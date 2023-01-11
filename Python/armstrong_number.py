import math as m

if __name__=="__main__":

    num = int(input("enter number for which you want to check is armstrong or not : "))

    temp_num = num
    temp=0
    temp_sum =0
    while temp_num !=0:
        temp = temp_num%10
        temp_num =int(temp_num/10)
        temp_sum = temp_sum + temp**3

    if temp_sum == num:
        print("it's Armstrong")
    else:
        print("it's not Armstrong")


