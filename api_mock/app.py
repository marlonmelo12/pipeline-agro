from flask import Flask, jsonify
import sqlite3
import pandas as pd

app = Flask(__name__)
DB_PATH = './api_data/chirps_data.db'

def load_data():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM precipitation ORDER BY date", conn)
    conn.close()
    grouped = df.groupby('date')
    return [group.to_dict('records') for _, group in grouped]

STREAM_DATA = load_data()
<<<<<<< HEAD

=======
>>>>>>> c12b6cae63b4907569f36ba8b7466d38a72573b2
CURRENT_DAY_INDEX = 0

@app.route('/stream/climate', methods=['GET'])
def get_climate_data():
    global CURRENT_DAY_INDEX
    
    if CURRENT_DAY_INDEX < len(STREAM_DATA):
        records_for_today = STREAM_DATA[CURRENT_DAY_INDEX]
    
        CURRENT_DAY_INDEX += 1
        
        return jsonify(records_for_today)
    else:
        return jsonify({"message": "End of data stream. Restart API to simulate again."}), 404

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)