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

UMA TABELA
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


DUAS TABELAS 
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
        FLOOR(EXTRACT(YEAR FROM AGE(current_date, TO_DATE(data_nascimento, 'YYYY-MM-DD')))) AS idade
    FROM prouni
),
sisu_idade AS (
    SELECT
        id_ies,
        lower(unaccent(nome_curso)) AS curso,
        FLOOR(EXTRACT(YEAR FROM AGE(current_date, TO_DATE(data_nascimento, 'YYYY-MM-DD')))) AS idade
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

-- Busca por cursos que contenham as palavras 'ANALISE' ou 'SISTEMA'
SELECT *
FROM prouni
WHERE curso ILIKE '%ANALISE%'
   OR curso ILIKE '%SISTEMA%';

-- Listagem de cursos distintos
SELECT DISTINCT nome_curso
FROM prouni;

-- Filtrar por instituição e curso específico
SELECT *
FROM prouni
WHERE id_ies = '15'
  AND curso ILIKE '%SISTEMAS ELETRICOS%';
