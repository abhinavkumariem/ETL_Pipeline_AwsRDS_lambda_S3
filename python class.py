class User:
    def __init__(self, user_id, name, email, age, signup_date, bank_balance, debt):
        self.user_id = user_id
        self.name = name
        self.email = email
        self.age = age
        self.signup_date = signup_date
        self.bank_balance = bank_balance
        self.debt = debt

    def email_current_balance(self):
        message = f"Dear {self.name},\n\nYour current balance is ${self.bank_balance:.2f}.\n\nBest regards,\nYour Bank"
        print(f"Sending email to {self.email}:\n{message}")

    @staticmethod
    def sort_users_by_name(users):
        # Simple bubble sort implementation
        n = len(users)
        for i in range(n):
            for j in range(0, n - i - 1):
                if users[j].name > users[j + 1].name:
                    users[j], users[j + 1] = users[j + 1], users[j]
        return users

# Example usage:
users = [
    User(1, "Alice", "alice@example.com", 30, "2022-01-01", 5000, 0),
    User(2, "Bob", "bob@example.com", 25, "2022-02-01", 3000, 1000),
    User(3, "Charlie", "charlie@example.com", 35, "2022-03-01", 10000, 2000)
]

# Demonstrate email_current_balance method
users[0].email_current_balance()

# Demonstrate sort_users_by_name static method
sorted_users = User.sort_users_by_name(users)
print("\nSorted users:")
for user in sorted_users:
    print(user.name)