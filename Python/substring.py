str ="Write a program that reads a string and display the longest substring of the given string having just the consonants"

char =input("Enter the substring that you would like to check in the string:")
if char in str:
    print("Substring is part of string")
else:
    print("Substring is not part of string")

#=================================================================== longest sub string ========================================================================================
print("\n=================================================================== longest sub string =====================================================================================\n")
str="Write a Program that read a string and display the longest sub string of the given string having just the consonent"
str_out=[]
long_word=""
max_len = 0
vol =['a','e','i','o','u']

str_lower = str.lower()
str_split = str_lower.split()

for var in str_split:
    str=""
    for char in var:
        if char in vol:
            pass
        else:
            str = str + char

    str_out.append(str)
    lenx = len(str)
    if max_len < lenx:
        long_word = str

print("Longest substring = ", long_word)