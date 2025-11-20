import json
import random
import time
import requests
import pandas as pd
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
from base64 import b64encode, b64decode

KEY = b"abcdefghijklmnop"  # 16-bytes key for AES-128

def encrypt(plain_text, key):
    """Funzione per criptare il testo usando AES in modalità ECB."""
    cipher = AES.new(key, AES.MODE_ECB)
    encrypted = cipher.encrypt(pad(plain_text.encode('utf-8'), AES.block_size))
    return b64encode(encrypted).decode('utf-8')


def get_random_user(max_retries=3, delay=0.5):
    """
    Chiama randomuser.me e restituisce un utente valido.
    Riprova alcune volte se la risposta non è valida.
    """
    for attempt in range(max_retries):
        r = requests.get("https://randomuser.me/api/", timeout=10)
        try:
            r.raise_for_status()
            data = r.json()

            # Controllo della risposta
            if "results" in data and isinstance(data["results"], list) and len(data["results"]) > 0:
                return data["results"][0]
            else:
                print(f"Risposta senza 'results' validi, tentativo {attempt + 1}")
        except Exception as e:
            print(f"Errore nella chiamata randomuser: {e} - tentativo {attempt + 1}")

        time.sleep(delay)

    # Se dopo max_retries non otteniamo un utente valido, solleviamo un errore
    raise RuntimeError("Impossibile ottenere un utente valido da randomuser.me dopo vari tentativi")


def main():
    """Funzione principale per leggere il CSV, generare utenti e salvarli in un nuovo CSV."""
    # 1. Leggo il CSV delle recensioni
    df = pd.read_csv("reviews_reduced.csv")

    # 2. Estraggo gli username unici (puliti)
    usernames = (
        df["username"]
        .dropna()
        .astype(str)
        .str.strip()
        .unique()
    )

    print(f"Username unici trovati: {len(usernames)}")

    records = []

    # 3. Genero utenti casuali per ogni username
    for uname in usernames:
        try:
            u = get_random_user()

            # Costruisco il record utente legato al tuo username
            record = {
                "username": uname,  # Il tuo username per la join con le reviews
                "name": u["name"]["first"], 
                "lastname": u["name"]["last"],
                "email": u["email"],
                "gender": u["gender"],
                "age": u["dob"]["age"],
                "city": u["location"]["city"],
                "country": u["location"]["country"],
                "password": encrypt(u["login"]["password"], KEY)
            }
            records.append(record)

            # per non stressare troppo l'API
            time.sleep(0.2)

        except Exception as e:
            print(f"Errore per username {uname}: {e}")

    # 4. Creo il DataFrame utenti
    df_users = pd.DataFrame(records)

    # 5. Salvo sul CSV
    df_users.to_csv("users_from_usernames.csv", index=False)

    print(f"Creato file users_from_usernames.csv con {len(df_users)} utenti.")


if __name__ == "__main__":
    main()
