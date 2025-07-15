AUNTHENTICATED = False
def require_authentication(func):
    def wrapper():
        if AUNTHENTICATED:
            func()
        else:
            print('Acess denied. Please try again')
    return wrapper
       

def login():
    global AUNTHENTICATED
    AUNTHENTICATED = True
    print("logged in.")

def logout():
    global AUNTHENTICATED
    AUNTHENTICATED = False
    print("Logged out")
    
@require_authentication
def view_dashboard():
        print("Welcome to your dashboard!.")

        
view_dashboard()
login()
view_dashboard()
logout()




