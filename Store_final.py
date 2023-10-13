from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from pydrive2.files import FileNotUploadedError

directorio_credenciales = 'credentials_module.json'

def login():
    GoogleAuth.DEFAULT_SETTINGS['client_config_file'] = directorio_credenciales
    gauth = GoogleAuth()
    gauth.LoadCredentialsFile(directorio_credenciales)
    
    if gauth.credentials is None:
        gauth.LocalWebserverAuth(port_numbers=[8092])
    elif gauth.access_token_expired:
        gauth.Refresh()
    else:
        gauth.Authorize()
        
    gauth.SaveCredentialsFile(directorio_credenciales)
    credenciales = GoogleDrive(gauth)
    return credenciales

def subir_archivo(ruta_archivo,id_folder):
    credenciales = login()
    archivo = credenciales.CreateFile({'parents': [{"kind": "drive#fileLink",\
                                                    "id": id_folder}]})
    archivo['title'] = ruta_archivo.split("/")[-1]
    archivo.SetContentFile(ruta_archivo)
    archivo.Upload()

if _name=='main_':
    subir_archivo('../dataset/spotify_dataset.csv','1KPtdh_iCBpPc_6WDjKLNetMzjGsYnDi-')


    #No me funciono el store ya que no se me creaba el credentials_module.json
    #Pero logre la autenticación con google pydrive