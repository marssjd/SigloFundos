# Guia rápido: Dashboards no Looker Studio

Este guia complementa o pipeline de dados e descreve como montar visualizações segmentadas por categoria, gestora ou grupo de fundos.

## 1. Preparar a fonte de dados

1. Acesse o [Looker Studio](https://lookerstudio.google.com/).
2. Crie uma nova fonte de dados e escolha **BigQuery** como conector.
3. Autentique-se com a conta que possui acesso ao projeto configurado no pipeline.
4. Selecione o dataset `curated` e a tabela desejada (ex.: `curated_cotas_por_categoria`).
5. Confirme o esquema sugerido e salve a fonte.

## 2. Construir páginas temáticas

Recomenda-se criar páginas separadas para diferentes perspectivas:

- **Visão por categoria CVM**: utilize `curated_cotas_por_categoria`.
- **Visão por gestora**: utilize `curated_cotas_por_gestora`.
- **Visão por grupos Looker**: utilize `curated_cotas_por_grupo_looker`.

Em cada página, adicione filtros de período (`data_cotacao`) e filtros específicos para a dimensão principal (categoria, gestora ou grupo).

## 3. Componentes sugeridos

- **Série temporal**: exiba a evolução de `valor_cota` e `patrimonio_liquido` ao longo do tempo.
- **Tabela com drill-down**: mostre métricas agregadas e permita ordenação por gestora/categoria.
- **Indicadores (scorecards)**: destaque variação percentual mês a mês.
- **Filtro por CNPJ**: permite focar em um fundo específico quando necessário.

## 4. Regras de atualização

- Configure a atualização automática da fonte de dados para ocorrer logo após o workflow mensal.
- Para análises retroativas, mantenha um controle de versões duplicando a fonte antes de alterações significativas.

## 5. Performance

- Prefira métricas pré-agregadas (camada `curated`) para dashboards públicos ou com grande audiência.
- Utilize os campos `grupo_looker` para segmentar fundos em grupos menores antes de criar gráficos detalhados.
- Ative o cache em componentes pesados (séries temporais longas) para reduzir chamadas ao BigQuery.

## 6. Checklist final

- [ ] Fonte de dados conectada ao projeto/dataset corretos.
- [ ] Filtros de período e categoria configurados.
- [ ] Painéis com títulos e descrições claras.
- [ ] Atualização automática agendada.
- [ ] Permissões de visualização revisadas (compartilhe apenas com usuários autorizados).

## 7. Troubleshooting

- **Dados não atualizados**: verifique se o workflow `monthly_ingest` executou com sucesso e se há novos arquivos em `output/`.
- **Colunas ausentes**: confirme o schema das tabelas no BigQuery e reimporte a fonte.
- **Erros de permissão**: atualize as credenciais no conector do Looker Studio e garanta acesso ao projeto BigQuery.

Com essas orientações, é possível montar relatórios consistentes e alinhados à estrutura de dados fornecida pelo pipeline Siglo Fundos.
