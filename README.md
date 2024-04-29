# Projeto Analytics Engineering:
---
# Engenharia de Dados e Garantia de Qualidade no Conjunto de Dados do Airbnb no Rio de Janeiro

Este projeto faz parte do módulo de Analytics Engineering do curso Santander Coders oferecido pela Ada Tech em parceria com o Banco Santander.

## Introdução à Base de Dados do Airbnb

O conjunto de dados "Inside Airbnb", disponível no website [Inside Airbnb](http://insideairbnb.com/), é uma valiosa fonte de informações sobre listagens de hospedagem, avaliações de hóspedes e disponibilidade de calendário em várias cidades ao redor do mundo, incluindo o Rio de Janeiro. Antes de prosseguirmos com a engenharia de dados, é importante entender os principais componentes deste conjunto de dados:

1. **Listing (Listagem):** Este conjunto de dados contém informações detalhadas sobre as propriedades listadas no Airbnb. Cada registro representa uma listagem individual e inclui informações como o tipo de propriedade, preço, localização, número de quartos, comodidades oferecidas e muito mais.

2. **Reviews (Avaliações):** O conjunto de dados de avaliações contém informações sobre as avaliações feitas por hóspedes que ficaram nas propriedades listadas. Ele inclui dados como a data da avaliação, o identificador da propriedade, os comentários escritos pelos hóspedes, e outras informações.

3. **Calendar (Calendário):** Este conjunto de dados contém informações sobre a disponibilidade das propriedades ao longo do tempo. Ele lista as datas em que as propriedades estão disponíveis para reserva, bem como os preços para cada data.

O dicionário dos dados também está disponível no website [Inside Airbnb](http://insideairbnb.com/).

## Passos do Projeto

> As etapas de 1 a 4 do projeto são executadas no script [***airbnb_rj_projeto_v2.ipynb***](airbnb_rj_projeto_v2.ipynb).

1. **Aquisição de Dados e Armazenamento de Dados em PostgreSQL - Camada Bronze**
   - Extração de dados do conjunto de dados "Inside Airbnb" e criação da Camada Landing através do script [**etl.py**](modules\etl.py).
   - Criação do banco de dados PostgreSQL e ingestão dos dados brutos das 3 tabelas ("Listing", "Reviews" e Calendar") na camada "bronze".

2. **Data Clean - Camada Silver:**
   - Tramentamento de valores ausentes, duplicatas e outliers nos dados brutos da camada "bronze".
   - Realização de limpeza textual em campos de texto.
   - As funções utlizadas nessas etapas estão no script [utils.py](modules\utils.py)

3. **Data Quality - Camada Silver:**
   - Definição e implementação das métricas de qualidade de dados, como integridade, precisão e consistência para os dados da camada "bronze".
   - Preenchimento dos campos nulos, padronização dos nomes e tipos das colunas, conversão de valores e  Verificação da precisão dos dados.

4. **Testes de Qualidade - Camada Silver:**
   - Utilizamos a biblioteca Great Expectations para criar testes de qualidade automatizados que verificam as expectativas definidas para os dados da camada "silver".
   - Desenvolvemos testes que asseguram que os dados da camada "silver" atendam às regras de negócios e aos requisitos de qualidade.

5. **Transformação de Dados com dbt - Camada Silver:**
   - Utilizamos a ferramenta dbt para criar a camada "silver" de dados, realizando transformações e preparando os dados da camada em questão.
   - Mantivemos um controle de versão dos modelos dbt relacionados à camada "silver" e automatizamos a execução das transformações.

6. **Armazenamento de Dados em PostgreSQL - Camada Silver:**
   - Armazenamos os dados da camada "silver" no mesmo banco de dados PostgreSQL.
   - Estabelecemos conexões entre o dbt e o PostgreSQL para carregar os dados transformados da camada "silver" no banco.

7. **Validação de Expectativas com Great Expectations - Camada Silver:**
   - Implementamos validações adicionais usando Great Expectations nas camadas de dados da camada "silver".
   - Monitoramos a qualidade dos dados da camada "silver" após cada transformação e ajustamos os testes de acordo.

8. **Transformação de Dados com dbt - Camada Gold:**
   - Utilizamos o dbt para criar a camada "gold" de dados, aplicando agregações especializadas, como médias de preços por propriedade, média de preço por ano e propriedade, quantidade de reviews por mês/ano de cada propriedade, e outras agregações especializadas.
   - Mantivemos um controle de versão dos modelos dbt relacionados à camada "gold" e automatizamos a execução das transformações.
   - Armazenamos os dados da camada "gold" no mesmo banco de dados PostgreSQL, mantendo a estrutura de dados otimizada para consultas analíticas.
