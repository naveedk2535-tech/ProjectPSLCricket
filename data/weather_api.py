"""
Open-Meteo weather client — FREE, no API key needed.
Fetches dew point, humidity, temperature for PSL venues.
Dew is a game-changer in T20 cricket — teams batting second
get a massive advantage when the ball is wet and hard to grip.
"""

import requests
from datetime import datetime, timedelta

import config
from database import db
from data.rate_limiter import can_call, record_call, check_cache, save_cache
from data.team_names import standardise_venue


def get_venue_coordinates(venue):
    """Get lat/lon for a PSL venue."""
    venue = standardise_venue(venue)
    for v_name, v_info in config.VENUES.items():
        if v_name == venue or v_info["city"] in venue:
            return v_info["lat"], v_info["lon"]
    return None, None


def get_match_weather(venue, match_date, match_time=None):
    """Fetch weather forecast for a specific match at a venue."""
    cache_key = f"weather_{venue}_{match_date}".replace(" ", "_")
    cached = check_cache(cache_key, config.CACHE_TTL["weather"])
    if cached:
        return cached

    if not can_call("open_meteo"):
        return cached

    lat, lon = get_venue_coordinates(venue)
    if lat is None:
        return None

    # Determine target hour (default 20:00 for evening matches)
    target_hour = 20
    if match_time:
        try:
            if "T" in match_time:
                target_hour = int(match_time.split("T")[1][:2])
            else:
                target_hour = int(match_time[:2])
        except (ValueError, IndexError):
            target_hour = 20

    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,relative_humidity_2m,dew_point_2m,wind_speed_10m,precipitation,cloud_cover",
        "start_date": match_date,
        "end_date": match_date,
        "timezone": "Asia/Karachi",
    }

    try:
        resp = requests.get(config.OPEN_METEO_BASE, params=params, timeout=10)
        record_call("open_meteo", f"forecast/{venue}", resp.status_code)

        if resp.status_code != 200:
            return None

        data = resp.json()
        hourly = data.get("hourly", {})

        if not hourly.get("time"):
            return None

        # Find the closest hour to match time
        idx = min(target_hour, len(hourly["time"]) - 1)

        weather = {
            "venue": venue,
            "match_date": match_date,
            "temperature": hourly["temperature_2m"][idx] if "temperature_2m" in hourly else None,
            "humidity": hourly["relative_humidity_2m"][idx] if "relative_humidity_2m" in hourly else None,
            "dew_point": hourly["dew_point_2m"][idx] if "dew_point_2m" in hourly else None,
            "wind_speed": hourly["wind_speed_10m"][idx] if "wind_speed_10m" in hourly else None,
            "precipitation": hourly["precipitation"][idx] if "precipitation" in hourly else None,
            "cloud_cover": hourly["cloud_cover"][idx] if "cloud_cover" in hourly else None,
        }

        # Calculate dew impact
        dew = calculate_dew_factor(weather, target_hour)
        weather.update(dew)

        save_cache(cache_key, weather)
        return weather

    except requests.RequestException as e:
        print(f"[Weather] Error: {e}")
        record_call("open_meteo", f"forecast/{venue}", 0)
        return None


def calculate_dew_factor(weather, match_hour=20):
    """
    Calculate dew impact on the match.
    Heavy dew makes the ball wet → harder to grip → bowling becomes harder in 2nd innings.
    Teams batting second get a significant advantage.

    Returns:
        dew_score: 0.0 to 1.0 (0 = no dew, 1 = extreme dew)
        heavy_dew: boolean
        batting_second_advantage: probability boost for team batting second
        summary: human-readable description
    """
    dew_point = weather.get("dew_point")
    humidity = weather.get("humidity")
    temp = weather.get("temperature")

    if dew_point is None or humidity is None:
        return {"dew_score": 0.0, "heavy_dew": 0, "batting_second_advantage": 0.0,
                "weather_summary": "Weather data unavailable"}

    is_evening = match_hour >= config.DEW_THRESHOLDS["heavy"]["evening_hour"]

    # Heavy dew conditions
    heavy = config.DEW_THRESHOLDS["heavy"]
    moderate = config.DEW_THRESHOLDS["moderate"]

    if (dew_point >= heavy["dew_point_min"] and humidity >= heavy["humidity_min"] and is_evening):
        dew_score = min(1.0, 0.7 + (dew_point - heavy["dew_point_min"]) * 0.03 + (humidity - heavy["humidity_min"]) * 0.005)
        return {
            "dew_score": round(dew_score, 2),
            "heavy_dew": 1,
            "batting_second_advantage": config.DEW_THRESHOLDS["batting_second_boost_heavy"],
            "weather_summary": f"HEAVY DEW expected. Dew point {dew_point}°C, humidity {humidity}%. "
                               f"Ball will be wet in 2nd innings. Bowling MUCH harder. "
                               f"Team batting second has significant advantage. "
                               f"Toss winner should BOWL first."
        }

    elif (dew_point >= moderate["dew_point_min"] and humidity >= moderate["humidity_min"] and is_evening):
        dew_score = 0.3 + (dew_point - moderate["dew_point_min"]) * 0.05
        return {
            "dew_score": round(min(0.7, dew_score), 2),
            "heavy_dew": 0,
            "batting_second_advantage": config.DEW_THRESHOLDS["batting_second_boost_moderate"],
            "weather_summary": f"Moderate dew likely. Dew point {dew_point}°C, humidity {humidity}%. "
                               f"Some impact on bowling in 2nd innings. "
                               f"Slight advantage to team batting second."
        }

    else:
        return {
            "dew_score": 0.0,
            "heavy_dew": 0,
            "batting_second_advantage": 0.0,
            "weather_summary": f"No significant dew expected. Temp {temp}°C, humidity {humidity}%. "
                               f"Conditions neutral for both innings."
        }


def save_weather_to_db(weather_data):
    """Save weather data to database."""
    if not weather_data:
        return

    db.execute(
        """INSERT INTO weather (venue, match_date, temperature, humidity, dew_point,
           wind_speed, precipitation, cloud_cover, heavy_dew, dew_score,
           weather_summary, fetched_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(venue, match_date) DO UPDATE SET
           temperature=excluded.temperature, humidity=excluded.humidity,
           dew_point=excluded.dew_point, wind_speed=excluded.wind_speed,
           heavy_dew=excluded.heavy_dew, dew_score=excluded.dew_score,
           weather_summary=excluded.weather_summary, fetched_at=excluded.fetched_at""",
        [weather_data["venue"], weather_data["match_date"],
         weather_data.get("temperature"), weather_data.get("humidity"),
         weather_data.get("dew_point"), weather_data.get("wind_speed"),
         weather_data.get("precipitation"), weather_data.get("cloud_cover"),
         weather_data.get("heavy_dew", 0), weather_data.get("dew_score", 0.0),
         weather_data.get("weather_summary", ""), db.now_iso()]
    )


def get_historical_weather(venue, match_date):
    """Fetch historical weather for backtesting dew model."""
    cache_key = f"weather_hist_{venue}_{match_date}".replace(" ", "_")
    cached = check_cache(cache_key)
    if cached:
        return cached

    lat, lon = get_venue_coordinates(venue)
    if lat is None:
        return None

    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,relative_humidity_2m,dew_point_2m,wind_speed_10m",
        "start_date": match_date,
        "end_date": match_date,
        "timezone": "Asia/Karachi",
    }

    # Use archive endpoint for historical data
    url = "https://archive-api.open-meteo.com/v1/archive"

    try:
        resp = requests.get(url, params=params, timeout=10)
        record_call("open_meteo", f"archive/{venue}", resp.status_code)

        if resp.status_code == 200:
            data = resp.json()
            hourly = data.get("hourly", {})
            if hourly.get("time"):
                idx = min(20, len(hourly["time"]) - 1)  # Evening hour
                result = {
                    "temperature": hourly.get("temperature_2m", [None])[idx],
                    "humidity": hourly.get("relative_humidity_2m", [None])[idx],
                    "dew_point": hourly.get("dew_point_2m", [None])[idx],
                    "wind_speed": hourly.get("wind_speed_10m", [None])[idx],
                }
                save_cache(cache_key, result)
                return result
    except requests.RequestException:
        pass

    return None
