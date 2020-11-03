import random

FI = open("words_alpha.txt", "r")
FO = open("big_words_and_nums.txt", "w")

for line in FI:
    FO.write(line.strip() + "\t" + str(random.randint(1, 10000)) + "\n")
    
FI.close()
FO.close()