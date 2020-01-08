import random
import csv

from faker import Faker

#Create 5000 administrators

fake = Faker(['en_US'])
usernames = set()
num_logs = 5529502
log_votes = {}

def get_unique_username(fake, usernames):

    name = fake.user_name()
    while name in usernames:
        name = fake.user_name()

    return name

def get_unique_upvote(upvotes, num_logs):

    x = random.randint(0, num_logs-1)
    while x in upvotes:
        x = random.randint(0, num_logs-1)

    return x


with open('admins.csv', 'w') as file:

    writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

    for _ in range(5000):
        uname = get_unique_username(fake, usernames)
        usernames.add(uname)
        email = fake.email()
        phone = fake.phone_number()
        upvotes = set()
        #Generate upvotes for at least 370 logs but not more than 1000
        num_votes = random.randint(370, 1000)
        for _ in range(num_votes):
            log = get_unique_upvote(upvotes, num_logs)
            upvotes.add(log)
            if log in log_votes:
                log_votes[log] += 1
            else:
                log_votes[log] = 1

        upvotes = sorted(list(upvotes))
        upvotes = ' '.join(str(i) for i in upvotes)

        writer.writerow([uname, email, phone, upvotes])

print("Distinct log votes: "+str(len(log_votes)))
print(str(len(log_votes))+"/"+str(num_logs)+" = "+str(len(log_votes)/num_logs))   