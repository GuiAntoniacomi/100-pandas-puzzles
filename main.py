import requests
from requests.auth import HTTPBasicAuth
from flask import Flask, request
import threading
import webbrowser
import time
import pandas as pd

# Variáveis Globais
app = Flask(__name__)
authorization_code = None

# Configurações OAuth
CLIENT_ID = '419759cc09659588aa42c22986968016e4ce2adc'
CLIENT_SECRET = 'ab1097471db5ec489f4e285b35098d95bb27c4469eb6abeabb99fecf46ea'
REDIRECT_URI = 'http://localhost:5000/callback'
STATE = "8bde85dd6e729dcd6e0d01dde003469d"
AUTHORIZATION_URL = f"https://www.bling.com.br/Api/v3/oauth/authorize?response_type=code&client_id={CLIENT_ID}&redirect_uri={REDIRECT_URI}&state={STATE}"
TOKEN_URL = "https://www.bling.com.br/Api/v3/oauth/token"
ID_DEPOSITO_ESCRITORIO = 863558208
PAGE_LIMIT = 500
MAX_PAGES = 218  # Limite de páginas para teste

@app.route('/callback')
def callback():
    global authorization_code
    authorization_code = request.args.get('code')
    return "Authorization code recebido. Você pode fechar esta janela."

def start_flask_app():
    app.run(port=5000)

def get_authorization_code(client_id, redirect_uri):
    threading.Thread(target=start_flask_app).start()
    time.sleep(1)
    
    url = f"https://www.bling.com.br/Api/v3/oauth/authorize?response_type=code&client_id={client_id}&redirect_uri={redirect_uri}&state={STATE}"
    webbrowser.open(url)

    # Aguarda até que o `authorization_code` seja definido
    while not authorization_code:
        time.sleep(1)
    
    return authorization_code

def get_access_token(client_id, client_secret, authorization_code):
    url = "https://www.bling.com.br/Api/v3/oauth/token"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "1.0"
    }
    body = {
        "grant_type": "authorization_code",
        "code": authorization_code,
        "redirect_uri": "http://localhost:5000/callback"
    }
    response = requests.post(url, headers=headers, data=body, auth=HTTPBasicAuth(client_id, client_secret))
    return response.json()

def fetch_all_products(access_token, page_limit, max_pages=None):
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    pagina = 1
    df_final = []

    while True:
        url = f"https://www.bling.com.br/Api/v3/produtos?pagina={pagina}&limite={page_limit}&criterio=1&tipo=T"
        response = requests.get(url, headers=headers)
        data = response.json()

        produtos = data.get('data', [])
        if not produtos or (max_pages and pagina > max_pages):
            break

        prod_df = pd.DataFrame(produtos)
        df_final.append(prod_df)

        pagina += 1

    return pd.concat(df_final, ignore_index=True), headers

# Função para consultar os saldos de estoque de múltiplos produtos
def consultar_saldos_produtos(prod_ids, headers):
    base_url = "https://www.bling.com.br/Api/v3/estoques/saldos"
    params = [('idsProdutos[]', prod_id) for prod_id in prod_ids]

    response = requests.get(base_url, headers=headers, params=params)
    if response.status_code == 200:
        data = response.json().get('data', [])
        saldos = []
        for item in data:
            produto_info = item.get('produto', {})
            saldo_escritorio = next(
                (d['saldoVirtual'] for d in item.get('depositos', []) if d['id'] == ID_DEPOSITO_ESCRITORIO),
                0  # Default para 0
            )
            saldos.append({
                'produto_id': produto_info.get('id', 'N/A'),
                'saldo_escritorio': saldo_escritorio
            })
        return saldos
    else:
        print(f"Erro ao acessar a API: {response.status_code}, {response.text}")
        return []

# Função para obter os saldos e adicionar ao DataFrame
def obter_estoque_produtos(prod_df, headers):
    batch_size = 20  # Tamanho do lote para cada requisição de saldo
    saldos = []

    for start in range(0, len(prod_df), batch_size):
        end = min(start + batch_size, len(prod_df))
        batch_df = prod_df.iloc[start:end]
        batch_ids = batch_df['id'].tolist()
        saldos += consultar_saldos_produtos(batch_ids, headers)

    saldos_df = pd.DataFrame(saldos)

    # Mesclar o DataFrame de produtos com os saldos de estoque
    return prod_df.merge(saldos_df, left_on='id', right_on='produto_id', how='left').drop(columns=['produto_id'])

# Main
if __name__ == "__main__":
    # Obtenha o authorization_code
    authorization_code = get_authorization_code(CLIENT_ID, REDIRECT_URI)
    token_response = get_access_token(CLIENT_ID, CLIENT_SECRET, authorization_code)
    access_token = token_response.get('access_token')

    # Obtenha todos os produtos
    df_combined, headers = fetch_all_products(access_token, PAGE_LIMIT, MAX_PAGES)

    # Adiciona a coluna de estoque ao DataFrame
    df_combined = obter_estoque_produtos(df_combined, headers)

    # Mostra as primeiras linhas do DataFrame combinado
    print(df_combined)
