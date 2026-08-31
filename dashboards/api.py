from flask import Flask, jsonify, request

app = Flask(__name__)

# Simple GET endpoint
@app.route("/api/hello", methods=["GET"])
def hello_world():
    return jsonify({"message": "Hello, Flask API is running!"})

# Example POST endpoint
@app.route("/api/data", methods=["POST"])
def receive_data():
    data = request.get_json()  # Expect JSON payload from client
    return jsonify({
        "received": data,
        "status": "success"
    })

# if __name__ == "__main__":
#     # Run on localhost:5000
#     app.run(host="0.0.0.0", port=5000, debug=True)

if __name__ == "__main__":
    print(f"Serving on http://localhost:{5001}")
    app.run(port=5001, debug=True)

