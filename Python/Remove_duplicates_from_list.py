print("Remove duplicates :")

list = [5,4,5,1,1,2,4,5,2,1,2]
out_lis =[]

for var in list:
    if var not in out_lis:
        out_lis.append(var)

print("After Removing duplicates",out_lis)