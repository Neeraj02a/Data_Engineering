# Find even value from input list

if __name__=='__main__':

    input_list = [1,2,3,4,5,6,4,7,7,9]
    output_list = []

    output_list = [var for var in input_list if var%2==0]
    print("Even numbers in the list :",output_list)

    # square of all the numbers from 1 to 9

    output_square_list =[var**2 for var in range(1,10)]
    print("Square of numbers ranges from 1-9 :",output_square_list)

    # dictionary of odd numbers where key as odd number and keys as cubes of odd numbers
    input_list = [1,2,3,4,5,6,7]
    output_dict={}

    output_dict = {keys:(keys**3) for keys in input_list if keys%2!=0}
    print("dictionary of odd values as keys and cubes of odd numbers as values :", output_dict)

    # states and there capitals

    states = ["Gujrat", "Maharashtra", "Rajasthan", "Bihar"]
    capitals = ["Gandhinagar", "Mumbai", "Jaipur", "Patna"]

    output_dict = {keys:values for keys,values in zip(states,capitals)}

    print("Dictionary of state and capitals :", output_dict)

    # ** if else conditional dictionary comprehension

    original_dict = {"jack":38, "mack":45, "bhoosan":20, "Rubika":50, "shradhha":60, "Ramesh":24, "suresh":31}

    new_dict = {keys:("old" if values > 40 else "young") for keys,values in original_dict.items()}
    print("new dictionary after performing operations :", new_dict)

    # nested dictionary comprehension

    output_dict = {k1:{k2:k1*k2 for k2 in range(1,6)} for k1 in range(2,5)}
    print("Nested dictionary comprehension :",output_dict)

    



