if __name__=='__main__':

    old_dic = {"A":67,"B":23,"C":45,"D":56,"E":12,"F":69,"G":67,"H":23}
    new_dic = {}

    for keys,values in old_dic.items():

        new_dic[values] = keys

    print("old dictionary before keys value pair got swap:", old_dic)
    print("new dictionary after keys value pair got swap :", new_dic)