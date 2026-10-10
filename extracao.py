# Extraindo dados da API de Clima: Open-Meteo
import requests


def extracao_dados():
    url = "https://api.open-meteo.com/v1/forecast"

    # Latitude e longitude de Belo Horizonte
    params = {
        "latitude": -19.9167,
        "longitude": -43.9345,
        "current": "temperature_2m,relative_humidity_2m,wind_speed_10m",
        "hourly": "temperature_2m,precipitation,relative_humidity_2m",
        "forecast_days": 1,
        "timezone": "America/Sao_Paulo"
    }

    try:
        response = requests.get(
            url,
            params=params,
            timeout=30
        )

        response.raise_for_status()

        dados = response.json()

        if not isinstance(dados, dict) or "hourly" not in dados:
            raise ValueError(
                "Resposta da API Open-Meteo sem o bloco 'hourly'."
            )

        return dados

    except requests.exceptions.RequestException as erro:
        print(f"Erro na requisição à Open-Meteo: {erro}")
        raise

    except ValueError as erro:
        print(f"Resposta inválida da Open-Meteo: {erro}")
        raise
