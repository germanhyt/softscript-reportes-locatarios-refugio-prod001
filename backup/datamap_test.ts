const reportes = [
    {
        reporteId: "ranking_global",
        script: "python3 /data/scripts/reporte1.py",
        imagenPath_destinatarios: [
            {
                imagenPath: "/data/scripts/reportes/ranking_global_mensual.png",
                destinatarios: [
                    "germanhuaytalla22@gmail.com",
                    "german.huaytalla@unmsm.edu.pe",
                ],
            },
        ],
    },
    {
        reporteId: "flujo_personas",
        script: "python3 /data/scripts/reporte2.py",
        imagenPath_destinatarios: [
            {
                imagenPath:
                    "/data/scripts/reportes2/flujo_personas_comparativo_2025_vs_2024.png",
                destinatarios: ["german.huaytalla@unmsm.edu.pe", "germanhuaytalla23@gmail.com"],
            },
        ],
    },
    {
        reporteId: "comparativo_ventas_semanal",
        script: "python3 /data/scripts/reporte3.py",
        imagenPath_destinatarios: [
            {
                imagenPath:
                    "/data/scripts/reportes3/reporte_comparativo_completo_anticuching.png",
                destinatarios: ["germanhuaytalla23@gmail.com"],
            },
        ],
    },
    {
        reporteId: "comparativo_ventas_mensual",
        script: "python3 /data/scripts/reporte4.py",
        imagenPath_destinatarios: [
            {
                imagenPath:
                    "/data/scripts/reportes4/analisis_anual_comparativo_anticuching_2025_vs_2024.png",
                destinatarios: [
                    "german.huaytalla@unmsm.edu.pe",
                    "germanhuaytalla23@gmail.come",
                ],
            },
        ],
    },
];

// return reportes;

export default reportes;