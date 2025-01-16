import folium

# Coordenadas aproximadas del centro de España
centro_espana = [40.4168, -3.7038]

# Crear un mapa centrado en España
mapa = folium.Map(location=centro_espana, zoom_start=6)

# Añadir un título encima del mapa
titulo_html = """
    <h3 style="font-size: 20px; text-align: center; font-family: Arial, sans-serif;">
        Mapa de España con polígonos
    </h3>
"""
mapa.get_root().html.add_child(folium.Element(titulo_html))

# Coordenadas de los vértices del primer polígono (por ejemplo, un triángulo)
coordenadas_poligono_1 = "38.32,-0.99 38.33,-0.92 38.36,-0.83 38.39,-0.86 38.4,-0.83 38.43,-0.81 38.44,-0.73 38.47,-0.67 38.5,-0.63 38.48,-0.61 38.48,-0.5 38.49,-0.45 38.53,-0.43 38.54,-0.4 38.57,-0.41 38.62,-0.33 38.59,-0.27 38.61,-0.21 38.61,-0.19 38.65,-0.16 38.62,-0.08 38.58,-0.06 38.54,-0.07 38.54,-0.15 38.48,-0.32 38.44,-0.37 38.41,-0.4 38.35,-0.41 38.36,-0.44 38.33,-0.51 38.3,-0.52 38.2,-0.51 38.19,-0.54 38.19,-0.59 38.14,-0.63 38.09,-0.64 37.99,-0.65 37.97,-0.69 37.94,-0.7 37.9,-0.75 37.85,-0.76 37.85,-0.78 37.88,-0.85 37.95,-0.92 38.01,-0.96 38.08,-1.02 38.14,-1.04 38.2,-0.99 38.26,-0.97 38.32,-0.99"
coordenadas = [tuple(map(float, pair.split(","))) for pair in coordenadas_poligono_1.split()]

# Dibujar el primer polígono con un tooltip
folium.Polygon(
    locations=coordenadas,
    color='blue',  # Color del borde
    fill=True,
    fill_color='red',  # Color de relleno
    fill_opacity=0.5,  # Opacidad del relleno
    tooltip="Polígono 1: Triángulo"  # Texto emergente
).add_to(mapa)

# Guardar el mapa en un archivo HTML
mapa.save("mapa_espana_con_titulo.html")

print("Mapa guardado como 'mapa_espana_con_titulo.html'")