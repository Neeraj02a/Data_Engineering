num_list = [1,2,3,4,5]
name_list = ["neeraj", "ashish", "nisha", "radhe", "vinod"]

print("number list :", num_list)
print("name list :", name_list)

name_list.append("bhoosan")

print("name list after append :", name_list)
name_list.insert(1,"krishna")
print("name list after insert :", name_list)

new_list = num_list + name_list

print("new list after concatination :")
print(new_list)

#new_list.extend(name_list)
#print("new list after extend :",new_list)

name_list.remove("krishna")
print("name list after use of remove function :", name_list)
name_list.pop()
print("name list after use of pop function :", name_list)
num_list.pop(1)
print("num list after use of pop function :", num_list)
name_list.reverse()
print("name list after using reverse function :", name_list)
name_list.sort()
print("name list after sorting :", name_list)
name_list.sort(reverse=True)
print("name list after reverse sorting :", name_list)
mylist = name_list.copy()
print("mylist after using copy statement :", mylist)
print("Length of name list is :", len(name_list))



