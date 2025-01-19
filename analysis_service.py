from flask import Flask, request, jsonify
import analysis

app = Flask(__name__)


@app.route('/analyze', methods=['POST'])
def analyze():
    try:
        filepath = request.json.get('filepath')
        analysis.process_csv_files(filepath)
        return jsonify({"message": "Analysis completed successfully!"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    app.run(port=5002, debug=True)
