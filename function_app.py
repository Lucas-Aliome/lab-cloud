import azure.functions as func
import math
import json

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="numericalintegral/{lower}/{upper}")
def NumericalIntegralFunction(req: func.HttpRequest) -> func.HttpResponse:
    # Récupération des paramètres de l'URL
    try:
        lower = float(req.route_params.get('lower'))
        upper = float(req.route_params.get('upper'))
    except (ValueError, TypeError):
        return func.HttpResponse("Paramètres invalides", status_code=400)
    
    # Algorithme de calcul de l'intégrale
    def compute_integral(l, u, N):
        area = 0.0
        dx = (u - l) / N
        for i in range(N):
            x = l + i * dx
            area += abs(math.sin(x)) * dx
        return area

    # Calculs pour les différentes valeurs de N demandées
    n_values = [10, 100, 1000, 10000, 100000, 1000000]
    results = {f"N={n}": compute_integral(lower, upper, n) for n in n_values}
    
    return func.HttpResponse(
        json.dumps(results),
        mimetype="application/json",
        status_code=200
    )