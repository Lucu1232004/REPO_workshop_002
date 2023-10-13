from pydrive.auth import GoogleAuth

def autenticar_con_google_drive():
    gauth = GoogleAuth()
    gauth.LocalWebserverAuth() # Crea un servidor web local y maneja automáticamente la autenticación.

if __name__ == "__main__":
    autenticar_con_google_drive()

directorio_credenciales = 'client_secrets.json'