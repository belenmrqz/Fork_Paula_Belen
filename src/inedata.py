class EsquemaINE:
    def __init__(self, codigo_tabla):
        self.codigo_tabla = codigo_tabla
        self.data = None
        self.esquema = None

    def obtener_datos(self):
        import requests
        try:
            r = requests.get(f"https://servicios.ine.es/wstempus/jsCache/ES/DATOS_TABLA/{self.codigo_tabla}", timeout=10)
            r.raise_for_status()
            self.data = r.json()
        except Exception as e:
            print(f"Error al obtener datos: {e}")
            self.data = None

    def tipo_simple(self, valor):
        if isinstance(valor, bool):
            return "BOOLEAN"
        elif isinstance(valor, int):
            return "INTEGER"
        elif isinstance(valor, float):
            return "FLOAT"
        elif isinstance(valor, str):
            return "STRING"
        elif valor is None:
            return "NULL"
        else:
            return str(type(valor))

    def imprimir_esquema(self, data=None):
        if data is None:
            data = self.data[0] if isinstance(self.data, list) else self.data
        esquema = {}
        if isinstance(data, dict):
            for k, v in data.items():
                if isinstance(v, dict):
                    esquema[k] = {"type": "OBJECT", "fields": self.esquema_simple(v)}
                elif isinstance(v, list) and v:
                    esquema[k] = {"type": "ARRAY", "element_type": self.esquema_simple(v[0])}
                elif isinstance(v, list):
                    esquema[k] = {"type": "ARRAY (empty)"}
                else:
                    esquema[k] = {"type": self.tipo_simple(v)}
        elif isinstance(data, list) and data:
            print(self.esquema_simple(data[0]))
        print(esquema)

    def generar_esquema(self):
        if self.data is None:
            self.obtener_datos()
        self.esquema = self.esquema_simple()
        return self.esquema
