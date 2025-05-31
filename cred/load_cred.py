from dotenv import load_dotenv
import os 

load_dotenv()

def load_creds_bq() -> list:

    BQ_DATASET = os.getenv('BQ_DATASET')
    BQ_PROJECT_ID = os.getenv('BQ_PROJECT_ID')
    
    creds_bq = {
        'BQ_DATASET':BQ_DATASET,
        'BQ_PROJECT_ID':BQ_PROJECT_ID
    }

    return creds_bq

def load_creds_mysql() -> list:

    HOST_MYSQL = os.getenv('HOST_MYSQL')
    PORT_MYSQL = os.getenv('PORT_MYSQL')
    USERNAME_MYSQL = os.getenv('USERNAME_MYSQL')
    PASSWORD_MYSQL = os.getenv('PASSWORD_MYSQL')

    creds_mysql ={ 
        'HOST_MYSQL':HOST_MYSQL,
        'PORT_MYSQL':PORT_MYSQL,
        'USERNAME_MYSQL':USERNAME_MYSQL,
        'PASSWORD_MYSQL':PASSWORD_MYSQL
    }

    return creds_mysql


print(load_creds_bq())
print('-----------------------------------------------------------------------')
print(load_creds_mysql())