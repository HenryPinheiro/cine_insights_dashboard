import requests
import pandas as pd
from time import sleep
import os
from dotenv import load_dotenv
load_dotenv()

API_KEY = os.getenv('API_KEY')
BASE_URL = 'https://api.themoviedb.org/3'
LANG = 'pt-BR'

def buscar(endpoint, pages=3):
    filmes = []
    for page in range(1, pages + 1):
        url = f'{BASE_URL}{endpoint}?api_key={API_KEY}&language={LANG}&page={page}'
        r = requests.get(url)
        if r.status_code != 200:
            print(f"Erro {r.status_code} na página {page}")
            break
        data = r.json()
        for item in data['results']:
            filmes.append({
                'id': item['id'],
                'titulo': item.get('title') or item.get('name'),
                'popularidade': item.get('popularity'),
                'nota_media': item.get('vote_average'),
                'votos': item.get('vote_count'),
                'data_lancamento': item.get('release_date'),
                'idioma_original': item.get('original_language'),
                'generos_ids': item.get('genre_ids'),
            })
        sleep(0.2)
    return filmes

def get_generos():
    url = f'{BASE_URL}/genre/movie/list?api_key={API_KEY}&language={LANG}'
    r = requests.get(url)
    data = r.json()
    return {g['id']: g['name'] for g in data['genres']}

# Criar pasta 'dados' se não existir
os.makedirs("dados", exist_ok=True)

# 1. Filmes populares
populares = buscar('/movie/popular')
df_populares = pd.DataFrame(populares)
df_populares.to_csv('dados/filmes_populares.csv', index=False, encoding='utf-8', sep=';', decimal=',')

# 2. Gêneros mais bem avaliados (com base nos populares)
generos_dict = get_generos()
df_generos = df_populares.explode('generos_ids')
df_generos['genero'] = df_generos['generos_ids'].map(generos_dict)
media_generos = df_generos.groupby('genero')['nota_media'].mean().reset_index()
media_generos = media_generos.sort_values(by='nota_media', ascending=False)
media_generos.to_csv('dados/generos_melhores.csv', index=False, encoding='utf-8', sep=';', decimal=',')

# 3. Dicionário de idiomas
idiomas_dict = {
    "en": "Inglês", "pt": "Português", "fr": "Francês", "es": "Espanhol", "ja": "Japonês",
    "ko": "Coreano", "it": "Italiano", "de": "Alemão", "ru": "Russo", "zh": "Chinês",
    "hi": "Hindi", "tr": "Turco", "ar": "Árabe", "xx": "Indefinido", "sv": "Sueco", "cn": "Chinês", "lv": "Latvian"
}

# 4. Tabela Mestra com os 300 melhores filmes
def buscar_top_filmes(pages=15): 
    filmes = []
    for page in range(1, pages + 1):
        url = f'{BASE_URL}/movie/top_rated?api_key={API_KEY}&language={LANG}&page={page}'
        r = requests.get(url)
        if r.status_code != 200:
            print(f"Erro na página {page}: {r.status_code}")
            continue
        data = r.json()
        for item in data['results']:
            filmes.append({
                'id': item['id'],
                'titulo': item.get('title'),
                'popularidade': item.get('popularity')
            })
        sleep(0.2)
    return filmes

def buscar_detalhes_filmes(filmes):
    detalhes = []
    for filme in filmes:
        id_filme = filme['id']
        url = f"{BASE_URL}/movie/{id_filme}?api_key={API_KEY}&language={LANG}&append_to_response=credits"
        r = requests.get(url)
        if r.status_code != 200:
            continue
        d = r.json()
        diretor = ""
        for pessoa in d.get("credits", {}).get("crew", []):
            if pessoa.get("job") == "Director":
                diretor = pessoa.get("name")
                break
        generos = [g['name'] for g in d.get('genres', [])]
        idioma_original = d.get('original_language')
        detalhes.append({
            'titulo': d.get('title'),
            'nota_media': d.get('vote_average'),
            'votos': d.get('vote_count'),
            'data_lancamento': d.get('release_date'),
            'idioma_original': idioma_original,
            'idioma': idiomas_dict.get(idioma_original, idioma_original),
            'generos': ", ".join(generos),
            'diretor': diretor,
            'descricao': d.get('overview'),
            'poster_url': f"https://image.tmdb.org/t/p/w500{d.get('poster_path')}" if d.get('poster_path') else None,
            'duracao_minutos': d.get('runtime'),
            'popularidade': filme['popularidade']
        })
        sleep(0.25)
    return pd.DataFrame(detalhes)

filmes_top300 = buscar_top_filmes(pages=15)
df_tabela_mestra = buscar_detalhes_filmes(filmes_top300)
df_tabela_mestra.to_csv('dados/tabela_mestra_filmes.csv', index=False, encoding='utf-8', sep=';', decimal=',')

print("✅ Tabelas atualizadas.")
