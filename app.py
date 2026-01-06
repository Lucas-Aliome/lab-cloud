import math
from flask import Flask, jsonify

app = Flask(__name__)

# Route de test (Racine) pour vérifier si le serveur répond
@app.route('/')
def home():
    return "Serveur Math Service en ligne ! Ajoutez /numericalintegralservice/0/3.14159 à l'URL."

def compute_integral(lower, upper, N):
    # Calcul de l'intégrale numérique de abs(sin(x))
    area = 0.0
    dx = (upper - lower) / N
    for i in range(N):
        x = lower + i * dx
        area += abs(math.sin(x)) * dx
    return area

@app.route('/numericalintegralservice/<float:lower>/<float:upper>', methods=['GET'])
def get_integral(lower, upper):
    # Valeurs de N demandées pour le TP
    n_values = [10, 100, 1000, 10000, 100000, 1000000]
    results = {}
    
    for n in n_values:
        res = compute_integral(lower, upper, n)
        results[f"N={n}"] = res
        
    # On utilise jsonify pour s'assurer que le format de sortie est correct
    return jsonify(results)

if __name__ == '__main__':
    # Le port 5000 est utilisé pour les tests locaux
    app.run(host='0.0.0.0', port=5000)