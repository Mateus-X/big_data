-- MODELAGEM DOS DADOS
CREATE TABLE prouni (
    ano SMALLINT NOT NULL,
    sigla_uf CHAR(2) NOT NULL,
    id_municipio INT,
    cpf CHAR(20) NOT NULL,
    sexo CHAR(1) NOT NULL,
    raca_cor SMALLINT,
    data_nascimento DATE,
    beneficiario_deficiente BOOLEAN,
    id_ies INT,
    campus VARCHAR(100),
    nome_municipio_ies VARCHAR(100),
    curso VARCHAR(150),
    turno_curso SMALLINT,
    tipo_bolsa SMALLINT,
    modalidade_ensino SMALLINT
);

CREATE TABLE sisu (
    ano SMALLINT NOT NULL,
    id_ies INT NOT NULL,
    sigla_ies VARCHAR(20),
    nome_curso VARCHAR(150),
    modalidade_concorrencia text,
    cpf CHAR(20) NOT NULL,
    sexo CHAR(1) NOT NULL,
    data_nascimento DATE
);

-- UMA TABELA
-- 
SELECT *
FROM sisu
WHERE sexo = 'F';

SELECT 
    sigla_ies,
    ARRAY_AGG(DISTINCT nome_curso) AS cursos_analise
FROM sisu
WHERE nome_curso ILIKE '%ANÁLISE%'
GROUP BY sigla_ies;

SELECT 
    sigla_ies,
    nome_curso,
    COUNT(*) AS total_pessoas
FROM sisu
GROUP BY sigla_ies, nome_curso;

SELECT 
    sigla_ies,
    AVG(total_pessoas) AS media_pessoas_por_curso
FROM (
    SELECT 
        sigla_ies,
        nome_curso,
        COUNT(*) AS total_pessoas
    FROM sisu
    GROUP BY sigla_ies, nome_curso
) AS qtd_por_curso_instituicao
GROUP BY sigla_ies
ORDER BY media_pessoas_por_curso DESC;


-- DUAS TABELAS 
-- Normaliza as tabelas e grava em tabelas auxiliares
CREATE TABLE prouni_norm AS
SELECT
    *,
    lower(unaccent(curso)) AS curso_norm,
    regexp_replace(cpf, '\D', '', 'g') AS cpf_norm
FROM prouni;

CREATE TABLE sisu_norm AS
SELECT
    *,
    lower(unaccent(nome_curso)) AS curso_norm,
    regexp_replace(cpf, '\D', '', 'g') AS cpf_norm
FROM sisu;

-- Prouni
CREATE TEMP TABLE prouni_ano AS
SELECT
    cpf_norm AS cpf,
    MAX(ano) AS ano_prouni
FROM prouni_norm
WHERE curso_norm LIKE '%analise e desenvolvimento de sistemas%'
GROUP BY cpf_norm;

-- Sisu
CREATE TEMP TABLE sisu_ano AS
SELECT
    cpf_norm AS cpf,
    MAX(ano) AS ano_sisu
FROM sisu_norm
WHERE curso_norm LIKE '%analise e desenvolvimento de sistemas%'
GROUP BY cpf_norm;

SELECT
    p.cpf,
    p.ano_prouni,
    s.ano_sisu
FROM prouni_ano p
JOIN sisu_ano s ON p.cpf = s.cpf
WHERE s.ano_sisu > p.ano_prouni
ORDER BY p.cpf;

-- União das duas tabelas com cálculo de idade
WITH prouni_idade AS (
    SELECT
        id_ies,
        lower(unaccent(curso)) AS curso,
        FLOOR(EXTRACT(YEAR FROM AGE(current_date, data_nascimento))) AS idade
    FROM prouni
),
sisu_idade AS (
    SELECT
        id_ies,
        lower(unaccent(nome_curso)) AS curso,
        FLOOR(EXTRACT(YEAR FROM AGE(current_date, data_nascimento))) AS idade
    FROM sisu
),
todas AS (
    SELECT * FROM prouni_idade
    UNION ALL
    SELECT * FROM sisu_idade
)
SELECT
    curso,
    id_ies,
    COUNT(idade) AS total_pessoas,
    ROUND(AVG(idade)::numeric, 2) AS media_idade
FROM todas
GROUP BY curso, id_ies
ORDER BY media_idade DESC;





-- Análise de memória
WITH prouni_idade AS (
    SELECT
        id_ies,
        lower(unaccent(curso)) AS curso,
        FLOOR(EXTRACT(YEAR FROM AGE(current_date, data_nascimento))) AS idade
    FROM prouni
),
sisu_idade AS (
    SELECT
        id_ies,
        lower(unaccent(nome_curso)) AS curso,
        FLOOR(EXTRACT(YEAR FROM AGE(current_date, data_nascimento))) AS idade
    FROM sisu
),
todas AS (
    SELECT * FROM prouni_idade
    UNION ALL
    SELECT * FROM sisu_idade
)
SELECT
    curso,
    id_ies,
    COUNT(idade) AS total_pessoas,
    ROUND(AVG(idade)::numeric, 2) AS media_idade
FROM todas
GROUP BY curso, id_ies
ORDER BY media_idade DESC;

-- Resultado
web_II=# EXPLAIN (ANALYZE, BUFFERS) WITH prouni_idade AS (
    SELECT
        id_ies,
        lower(unaccent(curso)) AS curso,
        FLOOR(EXTRACT(YEAR FROM AGE(current_date, data_nascimento))) AS idade
    FROM prouni
),
sisu_idade AS (
    SELECT
        id_ies,
        lower(unaccent(nome_curso)) AS curso,
        FLOOR(EXTRACT(YEAR FROM AGE(current_date, data_nascimento))) AS idade
    FROM sisu
),
todas AS (
    SELECT * FROM prouni_idade
    UNION ALL
    SELECT * FROM sisu_idade
)
SELECT
    curso,
    id_ies,
    COUNT(idade) AS total_pessoas,
    ROUND(AVG(idade)::numeric, 2) AS media_idade
FROM todas
GROUP BY curso, id_ies
ORDER BY media_idade DESC;                                                                           QUERY PLAN                                                           
                
------------------------------------------------------------------------------------------------------------------------------------------------
----------------
 Sort  (cost=217142.93..217242.93 rows=40000 width=76) (actual time=1616.789..1621.735 rows=1818.00 loops=1)
   Sort Key: (round(avg((floor(EXTRACT(year FROM age((CURRENT_DATE)::timestamp with time zone, (sisu.data_nascimento)::timestamp with time zone)
)))), 2)) DESC
   Sort Method: quicksort  Memory: 183kB
   Buffers: shared hit=7072 read=44332
   ->  Finalize GroupAggregate  (cost=201104.60..214085.38 rows=40000 width=76) (actual time=1611.783..1620.218 rows=1818.00 loops=1)
         Group Key: (lower(unaccent((sisu.nome_curso)::text))), sisu.id_ies
         Buffers: shared hit=7072 read=44332
         ->  Gather Merge  (cost=201104.60..212285.38 rows=96000 width=76) (actual time=1611.656..1617.726 rows=3117.00 loops=1)
               Workers Planned: 2
               Workers Launched: 2
               Buffers: shared hit=7072 read=44332
               ->  Sort  (cost=200104.58..200204.58 rows=40000 width=76) (actual time=1455.657..1455.816 rows=1039.00 loops=3)
                     Sort Key: (lower(unaccent((sisu.nome_curso)::text))), sisu.id_ies
                     Sort Method: quicksort  Memory: 281kB
                     Buffers: shared hit=7072 read=44332
                     Worker 0:  Sort Method: quicksort  Memory: 152kB
                     Worker 1:  Sort Method: quicksort  Memory: 66kB
                           ->  Partial HashAggregate  (cost=177917.97..197047.04 rows=40000 width=76) (actual time=1452.369..1453.346 rows=1039.00 loops=3)
                           Group Key: (lower(unaccent((sisu.nome_curso)::text))), sisu.id_ies
                           Planned Partitions: 4  Batches: 1  Memory Usage: 1553kB
                           Buffers: shared hit=7042 read=44332
                           Worker 0:  Batches: 1  Memory Usage: 1169kB
                           Worker 1:  Batches: 1  Memory Usage: 689kB
                           ->  Parallel Append  (cost=0.00..79556.52 rows=794840 width=68) (actual time=37.783..1261.406 rows=635729.33 loops=3)
                                 Buffers: shared hit=7042 read=44332
                                 ->  Parallel Seq Scan on sisu  (cost=0.00..72337.29 rows=755243 width=68) (actual time=8.964..1097.532 rows=604
052.00 loops=3)
                                       Buffers: shared hit=5411 read=44332
                                 ->  Parallel Seq Scan on prouni  (cost=0.00..3245.04 rows=55901 width=68) (actual time=45.092..160.193 rows=475
16.00 loops=2)
                                       Buffers: shared hit=1631
 Planning Time: 8.918 ms
 JIT:
   Functions: 36
   Options: Inlining false, Optimization false, Expressions true, Deforming true
   Timing: Generation 19.078 ms (Deform 5.966 ms), Inlining 0.000 ms, Optimization 10.944 ms, Emission 94.256 ms, Total 124.278 ms
 Execution Time: 1636.690 ms
(35 rows)

(END)


