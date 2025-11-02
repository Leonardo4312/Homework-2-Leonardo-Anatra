import os
import sys
import time
from pathlib import Path
from elasticsearch import Elasticsearch
from elasticsearch import helpers

ES_HOST = 'http://localhost:9200'
INDEX_NAME = 'file_indexer_pdf'
DIRECTORY_TO_INDEX = r"C:\Users\Leonardo\Desktop\FileDiTest"

def setup_index(es: Elasticsearch):
    index_mapping = {
        "mappings": {
            "properties": {
                "file_name": {
                    "type": "text",
                    "analyzer": "simple"
                },
                "content": {
                    "type": "text",
                    "analyzer": "italian"
                },
                "file_path": {
                    "type": "keyword" 
                }
            }
        }
    }

    try:
        if es.indices.exists(index=INDEX_NAME):
            print(f"Indice '{INDEX_NAME}' trovato. Lo elimino...")
            es.indices.delete(index=INDEX_NAME)
            
        print(f"Creo un nuovo indice '{INDEX_NAME}'...")
        es.indices.create(index=INDEX_NAME, body=index_mapping)
        print("Indice creato con successo.")
        
    except Exception as e:
        print(f"Errore durante la creazione dell'indice: {e}")
        sys.exit(1)


def generate_actions(directory_path, index_name):
    root_dir = Path(directory_path)
    file_paths = list(root_dir.rglob("*.txt"))
    
    print(f"\nTrovati {len(file_paths)} file .txt in '{directory_path}'")
    
    for path in file_paths:
        try:
            with open(path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            doc_source = {
                'file_name': path.name,
                'content': content,
                'file_path': str(path.absolute())
            }
            
            action = {
                "_index": index_name,
                "_id": str(path.absolute()),
                "_source": doc_source
            }
            yield action
            
        except Exception as e:
            print(f"Errore durante la lettura del file {path}: {e}")

def main():
    print("Avvio script di indicizzazione...")
    
    try:
        es = Elasticsearch(ES_HOST)
        es.info()
        print("Connessione a Elasticsearch stabilita.")
    except Exception as e:
        print(f"Errore: Impossibile connettersi a Elasticsearch a {ES_HOST}.")
        print(f"Dettagli: {e}")
        return

    setup_index(es)

    print("Avvio indicizzazione bulk...")
    start_time = time.time()
    
    actions_generator = generate_actions(DIRECTORY_TO_INDEX, INDEX_NAME)
    
    try:
        success, failed = helpers.bulk(es, actions_generator, chunk_size=500)
        
        print(f"Indicizzazione completata.")
        print(f"Documenti indicizzati con successo: {success}")
        print(f"Documenti falliti: {failed}")
        
        print("Eseguo il refresh dell'indice...")
        es.indices.refresh(index=INDEX_NAME)
        
        end_time = time.time()
        print(f"Tempo totale di indicizzazione: {end_time - start_time:.2f} secondi.")
        
    except Exception as e:
        print(f"Errore durante l'indicizzazione bulk: {e}")


if __name__ == "__main__":
    if not os.path.isdir(DIRECTORY_TO_INDEX):
        print(f"Errore: La directory '{DIRECTORY_TO_INDEX}' non esiste.")
        print("Modifica la variabile 'DIRECTORY_TO_INDEX' nello script.")
    else:
        main()