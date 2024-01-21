from flask import Flask, request, jsonify
import uuid
import os
import subprocess

app = Flask(__name__)

# Установите MAX_CONTENT_LENGTH, например, 256 мегабайт
app.config['MAX_CONTENT_LENGTH'] = 256 * 1024 * 1024  # 256 мегабайт в байтах


@app.route('/upload_csv', methods=['POST'])
def upload_csv():
    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400

    file = request.files['file']
    keyspace = request.form.get('keyspace')
    table = request.form.get('table')

    if file and keyspace and table:
        filename = f"{uuid.uuid4()}.csv"
        filepath = os.path.join('/root/upload_csv', filename)
        file.save(filepath)

        # Импорт в ScyllaDB
        try:
            subprocess.run([
                'cqlsh', '-e',
                f"COPY {keyspace}.{table} FROM '{filepath}' WITH HEADER=TRUE", 
            ], check=True)
            os.remove(filepath)
            return jsonify({"success": "File uploaded and imported to ScyllaDB"}), 200
        except subprocess.CalledProcessError as e:
            os.remove(filepath)
            return jsonify({"error": str(e)}), 500

    return jsonify({"error": "Missing data"}), 400

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5001, debug=True, threaded=True)