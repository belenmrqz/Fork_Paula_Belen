import json

import pandas as pd
import category_encoders as ce
import plotly.express as px
import plotly.graph_objects as go
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import r2_score, mean_absolute_error, root_mean_squared_error
import joblib
import os

# Modelos
from sklearn.linear_model import Ridge
from sklearn.neighbors import KNeighborsRegressor
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor


def train_housing_prediction():

    # 1. CARGA DE DATOS
    df = pd.read_csv("data_output/csv/ML.csv")

    print("--- Iniciando entrenamiento con Cross-Validation ---")

    # 2. SEPARAR VARIABLES
    y = df["precio_vivienda"]
    X = df.drop(columns=["precio_vivienda", "anio"])

    # 3. DIVISIÓN DEL DATASET
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    # 4. PREPROCESAMIENTO: Target Encoding y StandardScaler
    encoder = ce.TargetEncoder(cols=["comunidad"])

    # Aprende las medias SOLO del Train y transforma el Train
    X_train = encoder.fit_transform(X_train, y_train)

    # Transforma el Test usando lo que aprendió del Train
    X_test = encoder.transform(X_test)

    column_names = X_train.columns
    # Escalamos para los modelos en los que hace falta (Ridge, KNN)
    scaler = StandardScaler()
    X_train = scaler.fit_transform(X_train)
    X_test = scaler.transform(X_test)

    # 5. BÚSQUEDA DE MEJOR MODELO E HIPERPARÁMETOS (Grid Search + Cross Validation)
    models = {
        "Regresión Ridge": Ridge(),
        "k-NN Regressor": KNeighborsRegressor(),
        "Random Forest": RandomForestRegressor(random_state=42),
        "Gradient Boosting": GradientBoostingRegressor(random_state=42),
    }

    params = {
        "Regresión Ridge": {"alpha": [0.1, 1.0, 10.0]},  # Fuerza de la regularización
        "k-NN Regressor": {
            "n_neighbors": [3, 5, 7],  # Número de vecinos
            "weights": ["uniform", "distance"],
        },
        "Random Forest": {
            "n_estimators": [50, 100, 200],  # Número de árboles
            "max_depth": [3, 5, 7, 10],  # Profundidad máxima
            "min_samples_split": [5, 10, 15],  # Mínimo de muestras para dividir un nodo
            "min_samples_leaf": [2, 5, 10],  # Mínimo de muestras en la hoja final
        },
        "Gradient Boosting": {
            "n_estimators": [50, 100],  # Número de árboles
            "learning_rate": [
                0.05,
                0.1,
            ],  # Velocidad de aprendizaje (cuánto peso le da a cada árbol nuevo)
            "max_depth": [3, 5],  # Profundidad de cara árbol de corección
        },
    }

    results = []
    best_results={}
    best_r2_test = -1
    global_best_model = None
    best_model_name = ""

    for name, model in models.items():
        print(f"Entrenando y optimizando {name}...")

        grid = GridSearchCV(
            estimator=model,
            param_grid=params[name],
            cv=5,  # Hará validación cruzada dividiendo en 5 trozos
            n_jobs=-1,
            scoring="r2",  # Utliza el valor de r2 para elegir el parámetro mejor
        )

        # Entrenamos (prueba las combinaciones posibles)
        grid.fit(X_train, y_train)

        # Extraemos el mejor modelo que haya encontrado
        best_model = grid.best_estimator_
        print(f"\nMejores parámetros encontrados para {name}: \n{grid.best_params_}")

        # Predicciones en Train y Test
        y_pred_train = best_model.predict(X_train)
        y_pred_test = best_model.predict(X_test)

        # Métricas Train
        r2_train = r2_score(y_train, y_pred_train)
        mae_train = mean_absolute_error(y_train, y_pred_train)
        rmse_train = root_mean_squared_error(y_train, y_pred_train)

        # Métricas Test
        r2_test = r2_score(y_test, y_pred_test)
        mae_test = mean_absolute_error(y_test, y_pred_test)
        rmse_test = root_mean_squared_error(y_test, y_pred_test)

        results.append(
            {
                "Modelo": name,
                "R2_Train": r2_train,
                "R2_Test": r2_test,
                "MAE_Train": mae_train,
                "MAE_Test": mae_test,
                "RMSE_Train": rmse_train,
                "RMSE_Test": rmse_test,
            }
        )

        # Guardamos el modelo con mayor r2 (basado en el Test)
        if r2_test > best_r2_test:
            best_r2_test = r2_test
            global_best_model = best_model
            best_model_name = name
            best_results = results[-1]

    with open('data_output/models/model_metrics.json', 'w', encoding='utf-8') as f:
        json.dump(best_results, f)

    # 6. CLASIFICACIÓN DE MODELOS
    df_results = pd.DataFrame(results).sort_values("R2_Test", ascending=False)

    # Redondeamos a 4 decimales
    df_results_print = df_results.round(4)

    print("\nCLASIFICACIÓN FINAL DE MODELOS (TRAIN vs TEST)")
    print(df_results_print.to_string(index=False))
    print(
        f"\nMEJOR MODELO: {best_model_name.upper()} con un R2 en Test de {best_r2_test:.4f}"
    )

    # 7. IMPORTANCIA DE LAS VARIABLES
    if hasattr(global_best_model, "feature_importances_"):
        importances = global_best_model.feature_importances_
        feature_importance_df = pd.DataFrame(
            {"Variable": column_names, "Importancia": importances}
        )
        feature_importance_df = feature_importance_df.sort_values(
            by="Importancia", ascending=False
        )  # De mayor a menor para la consola

        print(f"\n--- Importancia de las Variables ({best_model_name}) ---")
        print(feature_importance_df.round(4).to_string(index=False))

    # 8. GENERACIÓN DE GRÁFICOS CON PLOTLY
    print("\nGenerando gráficos interactivos con Plotly...")

    # Gráfico 1: Comparativa del Torneo (Agrupado por Métrica)
    # 1. Filtramos solo las métricas de Test
    df_bars = df_results[["Modelo", "R2_Test", "MAE_Test", "RMSE_Test"]].copy()

    # 2. Agrupamos por métrica
    df_melted = df_bars.melt(id_vars="Modelo", var_name="Métrica", value_name="Valor")

    # 3. Creamos el gráfico agrupado
    fig_comp = px.bar(
        df_melted,
        x="Métrica",
        y="Valor",
        color="Modelo",
        barmode="group",
        title="<b>Comparativa de Modelos agrupados por Métrica</b>",
        color_discrete_sequence=px.colors.qualitative.Set2,
        text_auto=".2f",  # Pone el número encima de la barra
    )

    fig_comp.update_layout(
        xaxis_title="Métricas de Evaluación (Test)",
        yaxis_title="Puntuación / Error",
        legend_title_text="Modelos",
        legend=dict(x=1.02, y=1),  # Leyenda fuera del gráfico
    )

    fig_comp.write_html("data_output/graphics/comparativa_modelos.html")
    # fig_comp.show()

    # Gráfico 2: Importancia de las variables (Barra Horizontal)
    feature_importance_df = feature_importance_df.sort_values(
        by="Importancia", ascending=True
    )

    fig_importance = px.bar(
        feature_importance_df,
        x="Importancia",
        y="Variable",
        orientation="h",
        title="<b>¿Qué impacta más en el Precio de la Vivienda?</b>",
        color="Importancia",
        color_continuous_scale="Viridis",
        labels={"Importancia": "Peso en el Modelo (0 a 1)", "Variable": ""},
    )

    fig_importance.update_layout(showlegend=False, title_x=0.5)

    # Guardamos como HTML interactivo
    fig_importance.write_html("data_output/graphics/grafico_importancia_rf.html")
    # fig_importance.show()

    # Gráfico 3: Realidad vs Predicción (Scatter)
    y_pred_best_model = global_best_model.predict(X_test)

    df_pred = pd.DataFrame(
        {"Precio Real (IPV)": y_test, "Precio Predicho": y_pred_best_model}
    )

    fig_scatter = px.scatter(
        df_pred,
        x="Precio Real (IPV)",
        y="Precio Predicho",
        title=f"<b>{best_model_name}: Precio Real vs Precio Predicho</b>",
        opacity=0.7,
        color_discrete_sequence=["dodgerblue"],
        hover_data=["Precio Real (IPV)", "Precio Predicho"],
    )

    # Añadimos la línea diagonal de "Perfección"
    min_val = min(y_test.min(), y_pred_best_model.min())
    max_val = max(y_test.max(), y_pred_best_model.max())

    fig_scatter.add_trace(
        go.Scatter(
            x=[min_val, max_val],
            y=[min_val, max_val],
            mode="lines",
            name="Predicción Perfecta",
            line=dict(color="red", width=2, dash="dash"),
        )
    )
    fig_scatter.update_layout(
        title_x=0.5,
        legend=dict(
            orientation="h",  
            yanchor="top",
            y=-0.15,  
            xanchor="center",
            x=0.5,  
        ),
        margin=dict(r=20),
    )

    # Guardamos como HTML interactivo
    fig_scatter.write_html("data_output/graphics/grafico_prediccion_vs_real.html")
    # fig_scatter.show()

    return global_best_model, feature_importance_df, encoder, scaler


def save_model(regression_model, target_encoder, scaler):
    models_folder = "data_output/models"
    os.makedirs(models_folder, exist_ok=True)

    joblib.dump(regression_model, f"{models_folder}/regression_model.pkl")
    joblib.dump(target_encoder, f"{models_folder}/target_encoder.pkl")
    joblib.dump(scaler, f"{models_folder}/scaler.pkl")


if __name__ == "__main__":
    model, importance, encoder, scaler = train_housing_prediction()
    save_model(model, encoder, scaler)
