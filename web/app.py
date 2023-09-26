from flask import Flask, render_template
from datalayer.backend.es import ElasticsearchBackend 

app = Flask()

index_mapping = {
    "name":"test"
}
elk = ElasticsearchBackend(host='localhost', port=9200, index_mapping=index_mapping)

@app.route("/dashboard")
def display_dashboard():
    return render_template('index.html')
