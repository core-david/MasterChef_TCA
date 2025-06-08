CREATE OR REPLACE secure VIEW vista_reservas_expandidas AS
SELECT
  DATEADD(DAY, offset.value::INTEGER, r.fecha_llegada) AS fecha,
  r.habitaciones,
  r.numero_personas,
  r.numero_adultos,
  r.numero_menores,
  r.agencia,
  r.canal,
  r.segmento_alterno,
  r.tipo_habitacion_nombre
FROM
  clean_data r,
  LATERAL FLATTEN(input => ARRAY_GENERATE_RANGE(0, r.cantidad_noches)) AS offset;

CREATE OR REPLACE secure VIEW canales_uniques AS
SELECT DISTINCT(canal) FROM vista_reservas_expandidas;

CREATE OR REPLACE secure VIEW agencia_uniques AS
SELECT DISTINCT(agencia) FROM vista_reservas_expandidas;

CREATE OR REPLACE secure VIEW tipo_habitacion_uniques AS
SELECT DISTINCT(tipo_habitacion_nombre) FROM vista_reservas_expandidas;

create or replace secure view reservas_processed AS
SELECT
  fecha AS ds,
  SUM(habitaciones) AS ocupacion_total,
  sum(numero_personas) AS promedio_personas,
  sum(numero_adultos) AS promedio_adultos,
  sum(numero_menores) AS promedio_menores,
  MONTH(fecha) AS mes
FROM
  vista_reservas_expandidas
WHERE
  fecha BETWEEN '2019-02-13' AND '2020-04-29'
GROUP BY
  fecha
ORDER BY
  fecha;

create or replace secure view ds_uniques AS
SELECT COUNT(DISTINCT(rp.ds)) AS COUNT FROM my_streamlit.public.reservas_processed rp;

create or replace secure view ocupacion_total_metrics AS
SELECT 
    SUM(t.ocupacion_total) AS suma,
    ROUND(AVG(t.ocupacion_total), 0) AS promedio,
    MAX(t.ocupacion_total) AS maximo
FROM reservas_processed t;

create or replace secure view ocupacion_mensual AS
SELECT 
    rp.mes as mes,
    ROUND(AVG(rp.ocupacion_total), 0) AS ocupacion_mensual_promedio
FROM reservas_processed rp
GROUP BY rp.mes;
