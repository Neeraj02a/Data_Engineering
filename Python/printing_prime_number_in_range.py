print("Printing prime number in range (1-100)")

for num in range(1,101):
    count=0
    for i in range(2,num):
        if num%i==0:
            count=count +1

    if count<1:
        print(num)
