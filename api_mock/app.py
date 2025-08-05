from flask import Flask, jsonify
import sqlite3
import pandas as pd

app = Flask(__name__)
DB_PATH = './api_data/chirps_data.db'

# Carregar todos os dados na memória para simulação
def load_data():
    conn = sqlite3.connect(DB_PATH)
    # Ordenar por data para garantir a simulação cronológica
    df = pd.read_sql_query("SELECT * FROM precipitation ORDER BY date", conn)
    conn.close()
    # Agrupar por data
    grouped = df.groupby('date')
    return [group.to_dict('records') for _, group in grouped]

# Os dados são carregados uma vez quando o servidor inicia
STREAM_DATA = load_data()
# Ponteiro para saber qual dia "enviar"
CURRENT_DAY_INDEX = 0

@app.route('/stream/climate', methods=['GET'])
def get_climate_data():
    global CURRENT_DAY_INDEX
    
    if CURRENT_DAY_INDEX < len(STREAM_DATA):
        # Pega os registros do "dia atual"
        records_for_today = STREAM_DATA[CURRENT_DAY_INDEX]
        
        # Avança o ponteiro para o próximo dia na próxima chamada
        CURRENT_DAY_INDEX += 1
        
        return jsonify(records_for_today)
    else:
        # Quando os dados acabarem, retorne uma mensagem ou reinicie
        return jsonify({"message": "End of data stream. Restart API to simulate again."}), 404

if __name__ == '__main__':
    # Para reiniciar a simulação, basta reiniciar o servidor Flask
    app.run(host='0.0.0.0', port=5001, debug=True)