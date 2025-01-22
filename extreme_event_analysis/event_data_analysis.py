from sklearn.metrics import (
    confusion_matrix,
    cohen_kappa_score,
    precision_score,
)
from sklearn.cluster import DBSCAN
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

import seaborn as sns
from matplotlib import rc
import matplotlib.pyplot as plt

from datetime import datetime, timedelta
import ast
import pandas as pd
import geopandas as gpd
import contextily as ctx

import csv
import numpy as np
from shapely import Point, Polygon
import event_data_commons


class EventDataAnalysis:

    __EVENT_ID__ = ""
    __EVENT_NAME__ = ""
    __EVENT_START__ = datetime.now()
    __EVENT_END__ = datetime.now()

    __PRECIPITATION_ESTIMATIONS__ = ["uniforme", "severa", "extrema"]
    __CATEGORY_SIMPLE__ = ["Sin aviso", "Con aviso"]
    __CATEGORY_NAMES__ = ["Verde", "Amarillo", "Naranja", "Rojo"]

    __MAE__ = {}

    __DATAFRAME_EVENT_DATA__ = pd.DataFrame()
    __DATAFRAME_OBSERVED_COUNTS__ = pd.DataFrame()
    __DATAFRAME_WARNINGS_COUNTS__ = pd.DataFrame()
    __ANALYSIS_RESULTS__ = {}

    __FIELDS_EVENT_DATA__ = [
        "date",
        "geocode",
        "region",
        "area",
        "province",
        "polygon",
        "idema",
        "name",
        "latitude",
        "longitude",
        "altitude",
        "param_id",
        "param_name",
        "predicted_severity",
        "predicted_value",
        "region_severity",
        "region_value",
        "observed_severity",
        "observed_value",
    ]

    __FIELDS_OBSERVED_COUNTS__ = [
        "date",
        "geocode",
        "region",
        "area",
        "province",
        "polygon",
        "num_stations",
        "param_id",
        "param_name",
        "predicted_severity",
        "region_severity",
        "num_observed_severity0",
        "num_observed_severity1",
        "num_observed_severity2",
        "num_observed_severity3",
    ]

    __FIELDS_WARNINGS_COUNTS__ = [
        "date",
        "geocode",
        "region",
        "area",
        "province",
        "polygon",
        "param_id",
        "param_name",
        "num_predicted_severity0",
        "num_predicted_severity1",
        "num_predicted_severity2",
        "num_predicted_severity3",
        "num_region_severity0",
        "num_region_severity1",
        "num_region_severity2",
        "num_region_severity3",
    ]

    def __init__(
        self, event_id: str, event_name: str, event_start: datetime, event_end: datetime
    ):
        """
        Initializes an instance of the class.

        Parameters
        ----------
        event_id : str
            The identifier for the event to be analyzed.
        event_name : str
            The name of the event to be analyzed.
        event_start : datetime
            The start date and time of the event to be analyzed.
        event_end : datetime
            The end date and time of the event to be analyzed.
        """
        self.__EVENT_ID__ = event_id
        self.__EVENT_NAME__ = event_name
        self.__EVENT_START__ = event_start
        self.__EVENT_END__ = event_end

    def load_prepared_data(self):
        prepared_data = self.__prepare_event_data__(
            pd.read_csv(
                event_data_commons.get_path_to_file(
                    "event_prepared_data", event=self.__EVENT_ID__
                ),
                sep="\t",
                dtype=str,
            )
        )
        self.__DATAFRAME_EVENT_DATA__ = prepared_data

    def warning_level_comparison(self):
        observed_analysis = self.__DATAFRAME_EVENT_DATA__.groupby(
            ["date", "geocode", "param_id"], as_index=False
        ).agg(
            region=("region", "first"),
            area=("area", "first"),
            province=("province", "first"),
            polygon=("polygon", "first"),
            num_stations=("idema", "count"),
            param_name=("param_name", "first"),
            predicted_severity=("predicted_severity", "max"),
            region_severity=("region_severity", "max"),
            predicted_value=("predicted_value", "max"),
            region_value=("region_value", "max"),
        )
        severity_counts = (
            self.__DATAFRAME_EVENT_DATA__.groupby(["date", "geocode", "param_id"])[
                "observed_severity"
            ]
            .apply(lambda x: x.value_counts().reindex([0, 1, 2, 3], fill_value=0))
            .unstack(fill_value=0)
        )
        severity_counts.columns = [
            f"num_observed_severity{i}" for i in severity_counts.columns
        ]
        observed_analysis = observed_analysis.merge(
            severity_counts, on=["date", "geocode", "param_id"], how="left"
        )
        self.__DATAFRAME_OBSERVED_COUNTS__ = observed_analysis

        """predicted_severity_counts = (
            self.__DATAFRAME_EVENT_DATA__.groupby(["param_id"])["predicted_severity"]
            .apply(lambda x: x.value_counts().reindex([0, 1, 2, 3], fill_value=0))
            .unstack(fill_value=0)
        )
        predicted_severity_counts.columns = [
            f"num_predicted_severity{i}" for i in predicted_severity_counts.columns
        ]

        region_severity_counts = (
            self.__DATAFRAME_EVENT_DATA__.groupby(["param_id"])["region_severity"]
            .apply(lambda x: x.value_counts().reindex([0, 1, 2, 3], fill_value=0))
            .unstack(fill_value=0)
        )
        region_severity_counts.columns = [
            f"num_region_severity{i}" for i in region_severity_counts.columns
        ]

        warning_analysis = self.__DATAFRAME_EVENT_DATA__[["param_id", "param_name"]]
        warning_analysis = warning_analysis.drop_duplicates()
        warning_analysis = warning_analysis.merge(
            predicted_severity_counts, on=["param_id"], how="left"
        )
        warning_analysis = warning_analysis.merge(
            region_severity_counts, on=["param_id"], how="left"
        )

        self.__DATAFRAME_WARNINGS_COUNTS__ = warning_analysis"""

    def get_confusion_matrix(self):
        self.__standard_confusion_matrix__()
        self.__restricted_confusion_matrix__()
        self.__daily_confusion_matrix__()
        self.__parameter_confusion_matrix__()

    def __standard_confusion_matrix__(self):
        for e in range(len(self.__PRECIPITATION_ESTIMATIONS__)):

            plt.plot()
            plt.style.use("fivethirtyeight")
            plt.suptitle(
                f"{self.__EVENT_NAME__}; Matriz de confusión\nAvisos previstos y observados (por nivel)",
                fontsize=12,
                weight="bold",
                ha="center",
            )
            plt.title(
                f"Todas las regiones incluídas\nPrecipitaciones estimadas como {self.__PRECIPITATION_ESTIMATIONS__[e]}s",
                fontsize=8,
                ha="center",
            )

            data = self.__DATAFRAME_EVENT_DATA__.copy()
            data = data.groupby(["date", "geocode", "param_id"], as_index=False).agg(
                predicted_severity=("predicted_severity", "max"),
                region_severity=("region_severity", "max"),
            )
            data = data[
                (~data["param_id"].str.startswith("PR"))
                | (
                    data["param_id"].str.contains(
                        self.__PRECIPITATION_ESTIMATIONS__[e][:-2].upper()
                    )
                )
            ]

            data["predicted_severity"] = data["predicted_severity"].map(
                lambda x: self.__CATEGORY_NAMES__[int(x)]
            )
            data["region_severity"] = data["region_severity"].map(
                lambda x: self.__CATEGORY_NAMES__[int(x)]
            )
            severity_pred = data["predicted_severity"].to_list()
            severity_true = data["region_severity"].to_list()

            cm = confusion_matrix(
                y_pred=severity_pred,
                y_true=severity_true,
                normalize="all",
                labels=self.__CATEGORY_NAMES__,
            )
            self.__ANALYSIS_RESULTS__[
                f"Matriz de confusión; Todas; {self.__PRECIPITATION_ESTIMATIONS__[e]}"
            ] = precision_score(
                y_pred=severity_pred,
                y_true=severity_true,
                average=None,
                labels=self.__CATEGORY_NAMES__,
                zero_division=np.nan,
            )

            sns.heatmap(
                cm,
                annot=True,
                annot_kws={"size": 10},
                fmt=".3f",
                cbar=True,
                vmin=0,
                vmax=1,
                cmap="Blues",
                xticklabels=self.__CATEGORY_NAMES__,
                yticklabels=self.__CATEGORY_NAMES__,
                square=True,
                linecolor="white",
                linewidths=0.5,
            )
            plt.xlabel("Nivel de aviso previsión", weight="bold", fontsize=10)
            plt.ylabel("Nivel de aviso observado", weight="bold", fontsize=10)
            plt.yticks(fontsize=8, rotation=45, ha="right")
            plt.xticks(fontsize=8)
            plt.gcf().axes[-1].tick_params(labelsize=9)
            plt.tight_layout()

            path = event_data_commons.get_path_to_file(
                f"confusion-matrix", event=self.__EVENT_ID__
            )
            path = path.replace(
                event_data_commons.IMAGE_EXTENSION,
                f"-001_Categorias_TodasEstaciones_Precipitacion-{self.__PRECIPITATION_ESTIMATIONS__[e]}{event_data_commons.IMAGE_EXTENSION}",
            )
            plt.savefig(path, dpi=300, bbox_inches="tight")
            plt.close()
            plt.clf()

            plt.plot()
            plt.style.use("fivethirtyeight")
            plt.suptitle(
                f"{self.__EVENT_NAME__}; Matriz de confusión\nAvisos previstos y observados (existencia)",
                fontsize=12,
                weight="bold",
                ha="center",
            )
            plt.title(
                f"Todas las regiones incluídas\nPrecipitaciones estimadas como {self.__PRECIPITATION_ESTIMATIONS__[e]}s",
                fontsize=8,
                ha="center",
            )

            severity_pred = [
                (
                    self.__CATEGORY_SIMPLE__[1]
                    if s != self.__CATEGORY_NAMES__[0]
                    else self.__CATEGORY_SIMPLE__[0]
                )
                for s in severity_pred
            ]
            severity_true = [
                (
                    self.__CATEGORY_SIMPLE__[1]
                    if s != self.__CATEGORY_NAMES__[0]
                    else self.__CATEGORY_SIMPLE__[0]
                )
                for s in severity_true
            ]
            cm = confusion_matrix(
                y_pred=severity_pred,
                y_true=severity_true,
                normalize="all",
                labels=self.__CATEGORY_SIMPLE__,
            )
            self.__ANALYSIS_RESULTS__[
                f"Matriz de confusión; Todas; Binaria; {self.__PRECIPITATION_ESTIMATIONS__[e]}"
            ] = precision_score(
                y_pred=severity_pred,
                y_true=severity_true,
                average=None,
                labels=self.__CATEGORY_SIMPLE__,
                zero_division=np.nan,
            )

            sns.heatmap(
                cm,
                annot=True,
                annot_kws={"size": 10},
                fmt=".3f",
                cbar=True,
                vmin=0,
                vmax=1,
                cmap="Blues",
                xticklabels=self.__CATEGORY_SIMPLE__,
                yticklabels=self.__CATEGORY_SIMPLE__,
                square=True,
                linecolor="white",
                linewidths=0.5,
            )
            plt.xlabel("Previsión", weight="bold", fontsize=10)
            plt.ylabel("Observado", weight="bold", fontsize=10)
            plt.yticks(fontsize=8, rotation=45, ha="right")
            plt.xticks(fontsize=8)
            plt.gcf().axes[-1].tick_params(labelsize=9)
            plt.tight_layout()

            path = event_data_commons.get_path_to_file(
                f"confusion-matrix", event=self.__EVENT_ID__
            )
            path = path.replace(
                event_data_commons.IMAGE_EXTENSION,
                f"-000_SiNo_TodasEstaciones_Precipitacion-{self.__PRECIPITATION_ESTIMATIONS__[e]}{event_data_commons.IMAGE_EXTENSION}",
            )
            plt.savefig(path, dpi=300, bbox_inches="tight")
            plt.close()
            plt.clf()

    def __restricted_confusion_matrix__(self):
        for i in range(len(self.__PRECIPITATION_ESTIMATIONS__)):
            plt.plot()
            plt.style.use("fivethirtyeight")
            plt.suptitle(
                f"{self.__EVENT_NAME__}; Matriz de confusión\nAvisos previstos y observados (por nivel)",
                fontsize=12,
                weight="bold",
                ha="center",
            )
            plt.title(
                f"Solamente regiones efectadas por el evento\nPrecipitaciones estimadas como {self.__PRECIPITATION_ESTIMATIONS__[i]}s",
                fontsize=8,
                ha="center",
            )

            data = self.__DATAFRAME_EVENT_DATA__.copy()
            valid_geocodes = data[
                (data["predicted_severity"] > 0) | (data["region_severity"] > 0)
            ]["geocode"].unique()
            data = data[data["geocode"].isin(valid_geocodes)]

            data = data.groupby(["date", "geocode", "param_id"], as_index=False).agg(
                predicted_severity=("predicted_severity", "max"),
                region_severity=("region_severity", "max"),
            )
            data = data[
                (~data["param_id"].str.startswith("PR"))
                | (
                    data["param_id"].str.contains(
                        self.__PRECIPITATION_ESTIMATIONS__[i][:-2].upper()
                    )
                )
            ]

            data["predicted_severity"] = data["predicted_severity"].map(
                lambda x: self.__CATEGORY_NAMES__[int(x)]
            )
            data["region_severity"] = data["region_severity"].map(
                lambda x: self.__CATEGORY_NAMES__[int(x)]
            )
            severity_pred = data["predicted_severity"].to_list()
            severity_true = data["region_severity"].to_list()

            cm = confusion_matrix(
                y_pred=severity_pred,
                y_true=severity_true,
                normalize="all",
                labels=self.__CATEGORY_NAMES__,
            )
            self.__ANALYSIS_RESULTS__[
                f"Matriz de confusión; {self.__PRECIPITATION_ESTIMATIONS__[i]}"
            ] = precision_score(
                y_pred=severity_pred,
                y_true=severity_true,
                average=None,
                labels=self.__CATEGORY_NAMES__,
                zero_division=np.nan,
            )

            sns.heatmap(
                cm,
                annot=True,
                annot_kws={"size": 10},
                fmt=".3f",
                cbar=True,
                vmin=0,
                vmax=1,
                cmap="Blues",
                xticklabels=self.__CATEGORY_NAMES__,
                yticklabels=self.__CATEGORY_NAMES__,
                square=True,
                linecolor="white",
                linewidths=0.5,
            )
            plt.xlabel("Nivel de aviso previsión", weight="bold", fontsize=10)
            plt.ylabel("Nivel de aviso observado", weight="bold", fontsize=10)
            plt.yticks(fontsize=8, rotation=45, ha="right")
            plt.xticks(fontsize=8)
            plt.gcf().axes[-1].tick_params(labelsize=9)
            plt.tight_layout()

            path = event_data_commons.get_path_to_file(
                f"confusion-matrix", event=self.__EVENT_ID__
            )
            path = path.replace(
                event_data_commons.IMAGE_EXTENSION,
                f"-003_Categorias_Precipitacion-{self.__PRECIPITATION_ESTIMATIONS__[i]}{event_data_commons.IMAGE_EXTENSION}",
            )
            plt.savefig(path, dpi=300, bbox_inches="tight")
            plt.close()
            plt.clf()

            plt.plot()
            plt.style.use("fivethirtyeight")
            plt.suptitle(
                f"{self.__EVENT_NAME__}; Matriz de confusión\nAvisos previstos y observados (existencia)",
                fontsize=12,
                weight="bold",
                ha="center",
            )
            plt.title(
                f"Solamente regiones efectadas por el evento\nPrecipitaciones estimadas como {self.__PRECIPITATION_ESTIMATIONS__[i]}s",
                fontsize=8,
                ha="center",
            )

            severity_pred = [
                (
                    self.__CATEGORY_SIMPLE__[1]
                    if s != self.__CATEGORY_NAMES__[0]
                    else self.__CATEGORY_SIMPLE__[0]
                )
                for s in severity_pred
            ]
            severity_true = [
                (
                    self.__CATEGORY_SIMPLE__[1]
                    if s != self.__CATEGORY_NAMES__[0]
                    else self.__CATEGORY_SIMPLE__[0]
                )
                for s in severity_true
            ]
            cm = confusion_matrix(
                y_pred=severity_pred,
                y_true=severity_true,
                normalize="all",
                labels=self.__CATEGORY_SIMPLE__,
            )
            self.__ANALYSIS_RESULTS__[
                f"Matriz de confusión; Binaria; {self.__PRECIPITATION_ESTIMATIONS__[i]}"
            ] = precision_score(
                y_pred=severity_pred,
                y_true=severity_true,
                average=None,
                labels=self.__CATEGORY_SIMPLE__,
                zero_division=np.nan,
            )

            sns.heatmap(
                cm,
                annot=True,
                annot_kws={"size": 10},
                fmt=".3f",
                cbar=True,
                vmin=0,
                vmax=1,
                cmap="Blues",
                xticklabels=self.__CATEGORY_SIMPLE__,
                yticklabels=self.__CATEGORY_SIMPLE__,
                square=True,
                linecolor="white",
                linewidths=0.5,
            )
            plt.xlabel("Previsión", weight="bold", fontsize=10)
            plt.ylabel("Observado", weight="bold", fontsize=10)
            plt.yticks(fontsize=8, rotation=45, ha="right")
            plt.xticks(fontsize=8)
            plt.gcf().axes[-1].tick_params(labelsize=9)
            plt.tight_layout()

            path = event_data_commons.get_path_to_file(
                f"confusion-matrix", event=self.__EVENT_ID__
            )
            path = path.replace(
                event_data_commons.IMAGE_EXTENSION,
                f"-002_SiNo_Precipitacion-{self.__PRECIPITATION_ESTIMATIONS__[i]}{event_data_commons.IMAGE_EXTENSION}",
            )
            plt.savefig(path, dpi=300, bbox_inches="tight")
            plt.close()
            plt.clf()

    def __daily_confusion_matrix__(self):
        data = self.__DATAFRAME_EVENT_DATA__.copy()
        valid_geocodes = data[
            (data["predicted_severity"] > 0) | (data["region_severity"] > 0)
        ]["geocode"].unique()
        data = data[data["geocode"].isin(valid_geocodes)]

        for d in range((data["date"].max() - data["date"].min()).days):
            cm_date = data["date"].min() + timedelta(days=d)
            for i in range(len(self.__PRECIPITATION_ESTIMATIONS__)):
                plt.plot()
                plt.style.use("fivethirtyeight")
                plt.suptitle(
                    f"{self.__EVENT_NAME__}; Matriz de confusión\nAvisos previstos y observados (por nivel)\nDía {cm_date.strftime('%d/%m/%Y')}",
                    fontsize=12,
                    weight="bold",
                    ha="center",
                )
                plt.title(
                    f"Solamente regiones efectadas por el evento\nPrecipitaciones estimadas como {self.__PRECIPITATION_ESTIMATIONS__[i]}s",
                    fontsize=8,
                    ha="center",
                )

                data_subset = data[data["date"] == cm_date]
                data_subset = data_subset.groupby(
                    ["date", "geocode", "param_id"], as_index=False
                ).agg(
                    predicted_severity=("predicted_severity", "max"),
                    region_severity=("region_severity", "max"),
                )
                data_subset = data_subset[
                    (~data_subset["param_id"].str.startswith("PR"))
                    | (
                        data_subset["param_id"].str.contains(
                            self.__PRECIPITATION_ESTIMATIONS__[i][:-2].upper()
                        )
                    )
                ]

                data_subset["predicted_severity"] = data_subset[
                    "predicted_severity"
                ].map(lambda x: self.__CATEGORY_NAMES__[int(x)])
                data_subset["region_severity"] = data_subset["region_severity"].map(
                    lambda x: self.__CATEGORY_NAMES__[int(x)]
                )
                severity_pred = data_subset["predicted_severity"].to_list()
                severity_true = data_subset["region_severity"].to_list()

                cm = confusion_matrix(
                    y_pred=severity_pred,
                    y_true=severity_true,
                    normalize="all",
                    labels=self.__CATEGORY_NAMES__,
                )
                self.__ANALYSIS_RESULTS__[
                    f"Matriz de confusión; {cm_date.strftime('%Y%m%d')}; {self.__PRECIPITATION_ESTIMATIONS__[i]}"
                ] = precision_score(
                    y_pred=severity_pred,
                    y_true=severity_true,
                    average=None,
                    labels=self.__CATEGORY_NAMES__,
                    zero_division=np.nan,
                )

                sns.heatmap(
                    cm,
                    annot=True,
                    annot_kws={"size": 10},
                    fmt=".3f",
                    cbar=True,
                    vmin=0,
                    vmax=1,
                    cmap="Blues",
                    xticklabels=self.__CATEGORY_NAMES__,
                    yticklabels=self.__CATEGORY_NAMES__,
                    square=True,
                    linecolor="white",
                    linewidths=0.5,
                )
                plt.xlabel("Nivel de aviso previsión", weight="bold", fontsize=10)
                plt.ylabel("Nivel de aviso observado", weight="bold", fontsize=10)
                plt.yticks(fontsize=8, rotation=45, ha="right")
                plt.xticks(fontsize=8)
                plt.gcf().axes[-1].tick_params(labelsize=9)
                plt.tight_layout()

                path = event_data_commons.get_path_to_file(
                    f"confusion-matrix", event=self.__EVENT_ID__
                )
                path = path.replace(
                    event_data_commons.IMAGE_EXTENSION,
                    f"-004_Dia-{cm_date.strftime('%Y%m%d')}_Precipitacion-{self.__PRECIPITATION_ESTIMATIONS__[i]}{event_data_commons.IMAGE_EXTENSION}",
                )
                plt.savefig(path, dpi=300, bbox_inches="tight")
                plt.close()
                plt.clf()

    def __parameter_confusion_matrix__(self):
        data = self.__DATAFRAME_EVENT_DATA__.copy()
        valid_geocodes = data[
            (data["predicted_severity"] > 0) | (data["region_severity"] > 0)
        ]["geocode"].unique()
        data = data[data["geocode"].isin(valid_geocodes)]

        for p in data["param_id"].unique():
            cm_param = event_data_commons.MAPPING_PARAMETER_DESCRIPTION[p]
            data_subset = data[data["param_id"] == p]
            plt.plot()
            plt.style.use("fivethirtyeight")
            plt.suptitle(
                f"{self.__EVENT_NAME__}; Matriz de confusión\nAvisos previstos y observados (por nivel)\nParámetro: {event_data_commons.MAPPING_PARAMETER_ABBREVIATIONS[p]}",
                fontsize=12,
                weight="bold",
                ha="center",
            )
            plt.title(
                f"Solamente regiones efectadas por el evento",
                fontsize=8,
                ha="center",
            )

            data_subset = data_subset.groupby(["date", "geocode"], as_index=False).agg(
                predicted_severity=("predicted_severity", "max"),
                region_severity=("region_severity", "max"),
            )

            data_subset["predicted_severity"] = data_subset["predicted_severity"].map(
                lambda x: self.__CATEGORY_NAMES__[int(x)]
            )
            data_subset["region_severity"] = data_subset["region_severity"].map(
                lambda x: self.__CATEGORY_NAMES__[int(x)]
            )
            severity_pred = data_subset["predicted_severity"].to_list()
            severity_true = data_subset["region_severity"].to_list()

            cm = confusion_matrix(
                y_pred=severity_pred,
                y_true=severity_true,
                normalize="all",
                labels=self.__CATEGORY_NAMES__,
            )
            self.__ANALYSIS_RESULTS__[f"Matriz de confusión; {p}"] = precision_score(
                y_pred=severity_pred,
                y_true=severity_true,
                average=None,
                labels=self.__CATEGORY_NAMES__,
                zero_division=np.nan,
            )

            sns.heatmap(
                cm,
                annot=True,
                annot_kws={"size": 10},
                fmt=".3f",
                cbar=True,
                vmin=0,
                vmax=1,
                cmap="Blues",
                xticklabels=self.__CATEGORY_NAMES__,
                yticklabels=self.__CATEGORY_NAMES__,
                square=True,
                linecolor="white",
                linewidths=0.5,
            )
            plt.xlabel("Nivel de aviso previsión", weight="bold", fontsize=10)
            plt.ylabel("Nivel de aviso observado", weight="bold", fontsize=10)
            plt.yticks(fontsize=8, rotation=45, ha="right")
            plt.xticks(fontsize=8)
            plt.gcf().axes[-1].tick_params(labelsize=9)
            plt.tight_layout()

            path = event_data_commons.get_path_to_file(
                f"confusion-matrix", event=self.__EVENT_ID__
            )
            path = path.replace(
                event_data_commons.IMAGE_EXTENSION,
                f"-005_Parametro-{cm_param}{event_data_commons.IMAGE_EXTENSION}",
            )
            plt.savefig(path, dpi=300, bbox_inches="tight")
            plt.close()
            plt.clf()

    def get_distribution_chart(self):
        self.__distribution_chart__()

    def __distribution_chart__(self):
        data = self.__DATAFRAME_EVENT_DATA__.copy()
        data = data[
            (data["predicted_severity"] > 0)
            | (data["region_severity"] > 0)
            | (data["observed_severity"] > 0)
        ]
        data = data[
            [
                "date",
                "geocode",
                "param_id",
                "param_name",
                "predicted_severity",
                "region_severity",
            ]
        ]
        data["predicted_severity"] = data["predicted_severity"].map(
            event_data_commons.MAPPING_SEVERITY_TEXT
        )
        data["region_severity"] = data["region_severity"].map(
            event_data_commons.MAPPING_SEVERITY_TEXT
        )
        data = data.drop_duplicates()

        severity_counts_pred = (
            data.groupby("param_id")["predicted_severity"]
            .value_counts()
            .unstack(fill_value=0)
        )
        data_pred = (
            severity_counts_pred.reindex(
                columns=["verde", "amarillo", "naranja", "rojo"], fill_value=0
            )
            .apply(lambda row: row.tolist(), axis=1)
            .to_dict()
        )

        severity_counts_true = (
            data.groupby("param_id")["region_severity"]
            .value_counts()
            .unstack(fill_value=0)
        )
        data_true = (
            severity_counts_true.reindex(
                columns=["verde", "amarillo", "naranja", "rojo"], fill_value=0
            )
            .apply(lambda row: row.tolist(), axis=1)
            .to_dict()
        )

        category_colors = ["green", "yellow", "orange", "red"]

        bar_labels = [
            event_data_commons.MAPPING_PARAMETER_ABBREVIATIONS[lb]
            for lb in list(data_true.keys())
        ]
        bar_data = np.array(list(data_true.values()))
        bar_data_cum = bar_data.cumsum(axis=1)

        _, ax = plt.subplots()
        ax.invert_yaxis()
        ax.xaxis.set_visible(False)
        ax.set_xlim(0, np.sum(bar_data, axis=1).max())

        for i, (colname, color) in enumerate(
            zip(self.__CATEGORY_NAMES__, category_colors)
        ):
            widths = bar_data[:, i]
            starts = bar_data_cum[:, i] - widths
            ax.barh(
                bar_labels,
                widths,
                left=starts,
                height=0.5,
                label=colname,
                color=color,
                edgecolor="white",
                linewidth=0.5,
            )
            xcenters = starts + widths / 2

            for y, (x, c) in enumerate(zip(xcenters, widths)):
                ax.text(
                    x,
                    y,
                    str(int(c)),
                    ha="center",
                    va="center",
                    color="black",
                    fontsize=7,
                )
        ax.legend(
            ncol=len(self.__CATEGORY_NAMES__),
            bbox_to_anchor=(0, 1),
            loc="lower left",
            fontsize="small",
        )

        plt.style.use("fivethirtyeight")
        plt.ylabel("Parámetro", weight="bold", fontsize=9)
        plt.xlabel("Número de avisos", weight="bold", fontsize=10)
        plt.yticks(fontsize=8)
        plt.xticks(fontsize=8)
        plt.suptitle(
            f"{self.__EVENT_NAME__}; Gráfico de barras\nAvisos observados por parámetro",
            fontsize=14,
            weight="bold",
            ha="center",
        )

        path = event_data_commons.get_path_to_file(
            f"distribution-chart", event=self.__EVENT_ID__
        )
        path = path.replace(
            event_data_commons.IMAGE_EXTENSION,
            f"-001_Region{event_data_commons.IMAGE_EXTENSION}",
        )
        plt.savefig(path, dpi=300, bbox_inches="tight")
        plt.close()
        plt.clf()

        bar_labels = [
            event_data_commons.MAPPING_PARAMETER_ABBREVIATIONS[lb]
            for lb in list(data_pred.keys())
        ]
        bar_data = np.array(list(data_pred.values()))
        bar_data_cum = bar_data.cumsum(axis=1)

        _, ax = plt.subplots()
        ax.invert_yaxis()
        ax.xaxis.set_visible(False)
        ax.set_xlim(0, np.sum(bar_data, axis=1).max())

        for i, (colname, color) in enumerate(
            zip(self.__CATEGORY_NAMES__, category_colors)
        ):
            widths = bar_data[:, i]
            starts = bar_data_cum[:, i] - widths
            ax.barh(
                bar_labels,
                widths,
                left=starts,
                height=0.5,
                label=colname,
                color=color,
                edgecolor="white",
                linewidth=0.5,
            )
            xcenters = starts + widths / 2

            for y, (x, c) in enumerate(zip(xcenters, widths)):
                ax.text(
                    x,
                    y,
                    str(int(c)),
                    ha="center",
                    va="center",
                    color="black",
                    fontsize=7,
                )
        ax.legend(
            ncol=len(self.__CATEGORY_NAMES__),
            bbox_to_anchor=(0, 1),
            loc="lower left",
            fontsize="small",
        )

        plt.style.use("fivethirtyeight")
        plt.ylabel("Parámetro", weight="bold", fontsize=9)
        plt.xlabel("Número de avisos", weight="bold", fontsize=10)
        plt.yticks(fontsize=8)
        plt.xticks(fontsize=8)
        plt.suptitle(
            f"{self.__EVENT_NAME__}; Gráfico de barras\nAvisos previstos por parámetro",
            fontsize=14,
            weight="bold",
            ha="center",
        )

        path = event_data_commons.get_path_to_file(
            f"distribution-chart", event=self.__EVENT_ID__
        )
        path = path.replace(
            event_data_commons.IMAGE_EXTENSION,
            f"-002_Prevision{event_data_commons.IMAGE_EXTENSION}",
        )
        plt.savefig(path, dpi=300, bbox_inches="tight")
        plt.close()
        plt.clf()

    def __prepare_event_data__(self, data: pd.DataFrame) -> pd.DataFrame:
        data["date"] = pd.to_datetime(data["date"])
        data["latitude"] = pd.to_numeric(data["latitude"], errors="coerce")
        data["longitude"] = pd.to_numeric(data["longitude"], errors="coerce")
        data["altitude"] = pd.to_numeric(data["altitude"], errors="coerce")
        data["predicted_severity"] = pd.to_numeric(
            data["predicted_severity"], errors="coerce"
        ).astype(int)
        data["region_severity"] = pd.to_numeric(
            data["region_severity"], errors="coerce"
        ).astype(int)
        data["observed_severity"] = pd.to_numeric(
            data["observed_severity"], errors="coerce"
        ).astype(int)
        data["predicted_value"] = pd.to_numeric(
            data["predicted_value"], errors="coerce"
        ).astype(float)
        data["region_value"] = pd.to_numeric(
            data["region_value"], errors="coerce"
        ).astype(float)
        data["observed_value"] = pd.to_numeric(
            data["observed_value"], errors="coerce"
        ).astype(float)
        return data

    def get_analysis_stats(self):
        self.__mae__()
        self.__over_under_estimates__()
        self.__cohen_kappa_score__()

    def __mae__(self):
        data = self.__DATAFRAME_EVENT_DATA__
        data = data[(data["predicted_severity"] > 0) | (data["region_severity"] > 0)]
        self.__ANALYSIS_RESULTS__["MAE; Total"] = np.mean(
            np.abs(
                np.array(data["region_severity"]) - np.array(data["predicted_severity"])
            )
        )
        self.__ANALYSIS_RESULTS__["MAE; Total; Por estación"] = np.mean(
            np.abs(
                np.array(data["observed_severity"])
                - np.array(data["predicted_severity"])
            )
        )

        data_subset = data[
            (data["predicted_severity"] > 0) | (data["region_severity"] > 0)
        ]
        self.__ANALYSIS_RESULTS__["MAE; Total"] = np.mean(
            np.abs(
                np.array(data_subset["region_severity"])
                - np.array(data_subset["predicted_severity"])
            )
        )
        self.__ANALYSIS_RESULTS__["MAE; Por estación"] = np.mean(
            np.abs(
                np.array(data_subset["observed_severity"])
                - np.array(data_subset["predicted_severity"])
            )
        )

        for p in data_subset["param_id"].unique():
            data_param = data_subset[data_subset["param_id"] == p]
            self.__ANALYSIS_RESULTS__[f"MAE; {p}"] = np.mean(
                np.abs(
                    np.array(data_param["observed_severity"])
                    - np.array(data_param["predicted_severity"])
                )
            )

    def __over_under_estimates__(self):
        data = self.__DATAFRAME_EVENT_DATA__
        data["understimates"] = data["predicted_severity"] < data["region_severity"]
        data["overstimates"] = data["predicted_severity"] < data["region_severity"]
        self.__ANALYSIS_RESULTS__["Frecuencia de subestimación; Todas"] = data[
            "understimates"
        ].mean()
        self.__ANALYSIS_RESULTS__["Frecuencia de sobrestimación: Todas"] = data[
            "overstimates"
        ].mean()

        data_subset = data[
            (data["predicted_severity"] > 0) | (data["region_severity"] > 0)
        ]
        data_subset["understimates"] = (
            data_subset["predicted_severity"] < data_subset["region_severity"]
        )
        data_subset["overstimates"] = (
            data_subset["predicted_severity"] < data_subset["region_severity"]
        )
        self.__ANALYSIS_RESULTS__["Frecuencia de subestimación"] = data_subset[
            "understimates"
        ].mean()
        self.__ANALYSIS_RESULTS__["Frecuencia de sobrestimación"] = data_subset[
            "overstimates"
        ].mean()

        data_subset = data[
            (data["predicted_severity"] > 0) | (data["region_severity"] > 0)
        ]
        for p in data_subset["param_id"].unique():
            data_param = data_subset[data_subset["param_id"] == p]
            data_param["understimates"] = (
                data_param["predicted_severity"] < data_param["region_severity"]
            )
            data_param["overstimates"] = (
                data_param["predicted_severity"] < data_param["region_severity"]
            )
            self.__ANALYSIS_RESULTS__[f"Frecuencia de subestimación; {p}"] = data_param[
                "understimates"
            ].mean()
            self.__ANALYSIS_RESULTS__[f"Frecuencia de sobrestimación; {p}"] = (
                data_param["overstimates"].mean()
            )

    def __cohen_kappa_score__(self):
        data = self.__DATAFRAME_EVENT_DATA__
        self.__ANALYSIS_RESULTS__[f"Índice de Kappa de Cohen; Todas"] = (
            cohen_kappa_score(data["region_severity"], data["predicted_severity"])
        )

        data_subset = data[
            (data["predicted_severity"] > 0) | (data["region_severity"] > 0)
        ]
        self.__ANALYSIS_RESULTS__[f"Índice de Kappa de Cohen"] = cohen_kappa_score(
            data_subset["region_severity"], data_subset["predicted_severity"]
        )

        for p in data_subset["param_id"].unique():
            data_param = data_subset[data_subset["param_id"] == p]
            self.__ANALYSIS_RESULTS__[f"Índice de Kappa de Cohen; {p}"] = (
                cohen_kappa_score(
                    data_param["region_severity"], data_param["predicted_severity"]
                )
            )

    def get_error_map(self):
        self.__station_error_map__()
        self.__region_error_map__()

    def __station_error_map__(self):
        data = self.__DATAFRAME_EVENT_DATA__.copy()
        data = data[(data["predicted_severity"] > 0) | (data["region_severity"] > 0)]
        data = data[
            [
                "idema",
                "name",
                "latitude",
                "longitude",
                "predicted_severity",
                "observed_severity",
            ]
        ]
        data["name"] = data["idema"].astype(str) + " - " + data["name"]
        data["error"] = data["predicted_severity"] - data["observed_severity"]

        data_grouped = data.groupby(["name", "latitude", "longitude"], as_index=False)[
            "error"
        ].mean()
        gdf = gpd.GeoDataFrame(
            data_grouped,
            geometry=gpd.points_from_xy(
                data_grouped["longitude"], data_grouped["latitude"]
            ),
        )

        shape = gpd.read_file(event_data_commons.get_path_to_file("shapefile"))

        _, ax = plt.subplots()
        shape.plot(ax=ax, color="lightgrey", aspect=1, edgecolor="black")
        ax.set_xlim(
            data_grouped["longitude"].min() - 3, data_grouped["longitude"].max() + 3
        )
        ax.set_ylim(
            data_grouped["latitude"].min() - 3, data_grouped["latitude"].max() + 3
        )
        ctx.add_basemap(ax, source=ctx.providers.CartoDB.Positron)

        gdf.plot(
            ax=ax,
            column="error",
            cmap="coolwarm",
            legend=True,
            legend_kwds={"label": "Error medio (Previsto - Observado)"},
            markersize=5,
        )

        plt.xlabel("Longitud")
        plt.ylabel("Latitud")

        plt.style.use("fivethirtyeight")
        plt.yticks(fontsize=8)
        plt.xticks(fontsize=8)
        plt.suptitle(
            f"{self.__EVENT_NAME__}; Mapa de sobreestimaciones e infraestimaciones\nAvisos por estación meteorológica",
            fontsize=14,
            weight="bold",
            ha="center",
        )

        plt.tight_layout()

        path = event_data_commons.get_path_to_file(
            f"error-map", event=self.__EVENT_ID__
        )
        path = path.replace(
            event_data_commons.IMAGE_EXTENSION,
            f"-001_Estaciones{event_data_commons.IMAGE_EXTENSION}",
        )
        plt.savefig(path, dpi=300, bbox_inches="tight")
        plt.close()
        plt.clf()

    def __region_error_map__(self):
        data = self.__DATAFRAME_EVENT_DATA__.copy()
        data = data[(data["predicted_severity"] > 0) | (data["region_severity"] > 0)]
        data = data[
            [
                "geocode",
                "polygon",
                "predicted_severity",
                "region_severity",
                "longitude",
                "latitude",
            ]
        ]
        data["error"] = data["predicted_severity"] - data["region_severity"]

        data_grouped = data.groupby(["geocode", "polygon"], as_index=False)[
            "error"
        ].mean()
        data_grouped["geometry"] = data_grouped["polygon"].apply(
            lambda x: Polygon(
                [
                    (float(coord.split(",")[1]), float(coord.split(",")[0]))
                    for coord in x.split()
                ]
            )
        )

        gdf = gpd.GeoDataFrame(data_grouped, geometry="geometry")

        shape = gpd.read_file(event_data_commons.get_path_to_file("shapefile"))

        _, ax = plt.subplots()
        shape.plot(ax=ax, color="lightgrey", aspect=1, edgecolor="black")
        ax.set_xlim(data["longitude"].min() - 1, data["longitude"].max() + 1)
        ax.set_ylim(data["latitude"].min() - 1, data["latitude"].max() + 1)
        ctx.add_basemap(ax, source=ctx.providers.CartoDB.Positron)

        gdf.plot(
            ax=ax,
            column="error",
            cmap="coolwarm",
            legend=True,
            legend_kwds={"label": "Error medio (Previsto - Observado)"},
            edgecolor="black",
        )

        plt.xlabel("Longitud")
        plt.ylabel("Latitud")
        plt.style.use("fivethirtyeight")
        plt.yticks(fontsize=8)
        plt.xticks(fontsize=8)
        plt.suptitle(
            f"{self.__EVENT_NAME__}; Mapa de sobreestimaciones e infraestimaciones\nAvisos por región",
            fontsize=14,
            weight="bold",
            ha="center",
        )

        plt.tight_layout()

        path = event_data_commons.get_path_to_file(
            f"error-map", event=self.__EVENT_ID__
        )
        path = path.replace(
            event_data_commons.IMAGE_EXTENSION,
            f"-002_Regiones{event_data_commons.IMAGE_EXTENSION}",
        )
        plt.savefig(path, dpi=300, bbox_inches="tight")
        plt.close()
        plt.clf()

    def save_analisys_data(self):
        path = event_data_commons.get_path_to_file(
            "event_analysis", event=self.__EVENT_ID__
        )
        with open(path, mode="w", encoding="utf-8") as f:
            for key, value in self.__ANALYSIS_RESULTS__.items():
                if not isinstance(value, list):
                    value = [value]
                value_str = " ".join(map(str, value))
                f.write(f"{key} {value_str}\n")
