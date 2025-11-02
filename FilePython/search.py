import sys
from elasticsearch import Elasticsearch

ES_HOST = 'http://localhost:9200'
INDEX_NAME = 'file_indexer_pdf'

def parse_and_search(es: Elasticsearch, query_string: str):
    query_string = query_string.strip()
    
    #Ricerca per nome
    if query_string.startswith("nome "):
        field_to_search = "file_name"
        search_term = query_string[5:].strip()

    #Ricerca per contenuto
    elif query_string.startswith("contenuto "):
        field_to_search = "content"
        search_term = query_string[10:].strip()
    else:
        print("Sintassi non valida.")
        return

    # Controlla se è una "Phrase Query" (racchiusa tra virgolette)
    if search_term.startswith('"') and search_term.endswith('"'):
        # È una "match_phrase" query
        search_term = search_term[1:-1] # Rimuovi le virgolette
        query_type = "match_phrase"
        query_body = {
            "query": {
                "match_phrase": {
                    field_to_search: search_term
                }
            }
        }
        print(f"Ricerca PHRASE su '{field_to_search}' per: '{search_term}'")
        
    else:
        # È una "match" query
        query_type = "match"
        query_body = {
            "query": {
                "match": {
                    field_to_search: search_term
                }
            }
        }
        print(f"Ricerca MATCH su '{field_to_search}' per: '{search_term}'")


    try:
        response = es.search(index=INDEX_NAME, body=query_body)
        
        hits = response['hits']['hits']
        total_hits = response['hits']['total']['value']
        
        print("-" * 30)
        print(f"Trovati {total_hits} risultati:")
        
        if total_hits == 0:
            print("Nessun documento corrisponde alla ricerca.")
            return

        
        for i, hit in enumerate(hits):
            score = hit['_score']
            source_doc = hit['_source']
            
            filename = source_doc.get('file_name', 'N/A')
            filepath = source_doc.get('file_path', 'N/A')
                      
            # Stampiamo i risultati
            print(f"\n{i+1}. File: {filename} (Score: {score:.2f})")
            print(f"   Percorso: {filepath}")
            
            
            if 'content' in source_doc:
                content_preview = source_doc['content'].replace('\n', ' ').strip()
                print(f"   Preview: {content_preview[:150]}...")


    except Exception as e:
        print(f"Errore durante la ricerca: {e}")


def main():
    print("Avvio script di ricerca...")
    
    try:
        es = Elasticsearch(ES_HOST)
        if not es.indices.exists(index=INDEX_NAME):
            print(f"Errore: L'indice '{INDEX_NAME}' non esiste.")
            sys.exit()
            
        print(f"Connesso a Elasticsearch e pronto a cercare sull'indice '{INDEX_NAME}'.")
        
    except Exception as e:
        print(f"Errore: Impossibile connettersi a Elasticsearch a {ES_HOST}.")
        print(f"Dettagli: {e}")
        return

    print("Digita 'esci' o 'exit' per terminare.")
    print("Sintassi: nome <termini>  |  contenuto <termini>")
    print("-" * 40)
    
    while True:
        try:
            query = input("\nQuery> ")
            
            if query.lower() in ['esci', 'exit', 'quit']:
                break
                
            if not query:
                continue
                
            parse_and_search(es, query)
            
        except KeyboardInterrupt:
            print("\nArrivederci!")
            break
        except Exception as e:
            print(f"Errore inaspettato: {e}")


if __name__ == "__main__":
    main()