# Instalar as bibliotecas necessárias

import dash
from dash import dcc, html, dash_table
import pandas as pd
import requests
import chardet
import schedule
import time
import threading
import pytz
import os
from datetime import datetime
from dash.dependencies import Input, Output, State
import flask
from flask import Flask

# 🔹 1. VARIÁVEIS GLOBAIS 🔹
url = "https://dadosabertos-download.cgu.gov.br/e-Ouv/manifestacoes-ouvidoria.csv"
arquivo_original = "manifestacoes_original.csv"
arquivo_utf8 = "manifestacoes_utf8.csv"
ultima_atualizacao = ""

chunk_size = 10000  # Lê o arquivo em blocos de 10 mil linhas para evitar alto consumo de memória
orgao_filtro = [
    "Secretaria Municipal de Segurança e Ordem Pública",
    "FLORAM - Fundação Municipal do Meio Ambiente",
    "Pró-Cidadão",
    "Secretaria Municipal da Fazenda",
    "Secretaria Municipal do Continente",
    "Secretaria Municipal de Saúde",
    "Secretaria Municipal de Educação",
    "PROCON",
    "Secretaria Municipal da Assistência Social",
    "Secretaria Municipal de Cultura, Esporte e Juventude",
    "IPUF - Instituto de Pesquisa e Planejamento Urbano",
    "IPREF - Instituto de Previdência de Florianópolis",
    "Procuradoria Geral do Município",
    "Secretaria Municipal de Meio Ambiente e Desenvolvimento Sustentável",
    "Gabinete do Prefeito",
    "Secretaria Municipal de Planejamento, Habitação e Desenvolvimento Urbano",
    "Guarda Municipal",
    "Secretaria Municipal de Planejamento e Inteligência Urbana",
    "Defesa Civil de Florianópolis",
    "Secretaria Municipal de Infraestrutura e Manutenção da Cidade",
    "Secretaria Municipal da Casa Civil",
    "Secretaria Municipal de Limpeza e Manutenção Urbana",
    "FCFFC - Fundação Cultural de Florianópolis Franklin Cascaes",
    "FME - Fundação Municipal de Esportes",
    "IGEOF - Instituto de Geração de Oportunidades de Florianópolis",
    "Prefeitura - Ouvidoria Geral",
    "Secretaria Municipal de Cultura, Esporte e Lazer",
    "Secretaria Municipal de Governo",
    "Secretaria Municipal de Licitações, Contratos e Parcerias",
    "SOMAR - Fundação Rede Solidária Somar Floripa",
    "Secretaria Municipal de Turismo, Tecnologia e Desenvolvimento Econômico",
    "Secretaria Municipal de Administração"
]
# 🔹 2. FUNÇÃO PARA BAIXAR O ARQUIVO 🔹
def baixar_arquivo():
    global ultima_atualizacao

    try:
        response = requests.get(url, timeout=60)
        if response.status_code == 200:
            with open("manifestacoes_original.csv", "wb") as file:
                file.write(response.content)
            print("✅ Arquivo baixado com sucesso!")  # Depuração
        else:
            raise Exception("❌ Erro ao baixar via requests")

    except Exception:
        os.system(f"wget -O manifestacoes_original.csv {url}")
        if not os.path.exists("manifestacoes_original.csv") or os.path.getsize("manifestacoes_original.csv") == 0:
            print("❌ Erro: arquivo não encontrado após tentativa de download!")

import os

if not os.path.exists("manifestacoes_utf8.csv"):
    print("❌ ERRO: Arquivo 'manifestacoes_utf8.csv' não encontrado!")
    baixar_arquivo()  # Tenta baixar novamente
    
# 🔹 3. FUNÇÃO PARA PROCESSAR OS DADOS 🔹
def atualizar_dados():
    global df, ultima_atualizacao

    baixar_arquivo()

    with open("manifestacoes_original.csv", "rb") as f:
        resultado = chardet.detect(f.read(10000))
    codificacao_detectada = resultado["encoding"]

    with open("manifestacoes_original.csv", "r", encoding=codificacao_detectada, errors="replace") as f_in, \
         open("manifestacoes_utf8.csv", "w", encoding="utf-8") as f_out:
        for line in f_in:
            f_out.write(line)

    print("✅ Arquivo convertido para UTF-8 com sucesso!")  # Depuração

primeiro_chunk = True  # Para saber se precisa escrever o cabeçalho

for chunk in pd.read_csv(arquivo_utf8, sep=";", encoding="utf-8", low_memory=True, dtype=str, chunksize=chunk_size):
    chunk = chunk[chunk["Esfera"] == "Municipal"]  # Filtra esfera Municipal
    chunk = chunk[chunk["Nome Órgão"].isin(orgao_filtro)]  # Filtra órgãos permitidos
    
    # Salvar diretamente no arquivo sem acumular na memória
    chunk.to_csv(arquivo_filtrado, mode="w" if primeiro_chunk else "a", sep=";", index=False, encoding="utf-8", header=primeiro_chunk)
    primeiro_chunk = False  # Apenas o primeiro chunk escreve o cabeçalho
    
    # Criar DataFrame processado diretamente
    df_list = []
    
    # 🔹 Identificar colunas que contêm datas e converter corretamente
    colunas_data = [col for col in df.columns if "Data" in col or "data" in col]

    for col in colunas_data:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True).dt.strftime("%d/%m/%Y")

    # 🔹 Criar a coluna "Ano" com base na "Data Registro"
    if "Data Registro" in df.columns:
        df["Ano"] = pd.to_datetime(df["Data Registro"], errors="coerce", dayfirst=True).dt.year

    # 🔹 REORDENAR AS COLUNAS 🔹
    colunas_ordenadas = ["Ano", "Esfera", "Nome Órgão", "Tipo Manifestação", "Assunto"] + \
                        [col for col in df.columns if col not in ["Ano", "Esfera", "Nome Órgão", "Tipo Manifestação", "Assunto"]]
    df = df[colunas_ordenadas]

    fuso_brasilia = pytz.timezone("America/Sao_Paulo")
    ultima_atualizacao = datetime.now(fuso_brasilia).strftime("%d/%m/%Y %H:%M")
    
# 🔹 4. AGENDANDO ATUALIZAÇÃO DIÁRIA 🔹
def iniciar_agendamento():
    schedule.every().day.at("01:00").do(atualizar_dados)
    while True:
        schedule.run_pending()
        time.sleep(60)

atualizar_dados()
thread = threading.Thread(target=iniciar_agendamento, daemon=True)
thread.start()

# 🔹 5. INICIAR DASH 🔹
server = flask.Flask(__name__)
app = dash.Dash(__name__, server=server)
app.title = "FalaBR - Registros de Manifestações (Prefeitura de Florianópolis/SC)"

# 🔹 6. DEFINIR OS FILTROS 🔹
filtros = [
    "Ano", "Nome Órgão", "Tipo Manifestação", "Assunto",
    "Município Manifestante", "UF do Município Manifestante",
    "Município Manifestação", "UF do Município Manifestação"
]
filtros_adicionais = [col for col in df.columns if col not in filtros and col != "Data Registro"]

# 🔹 7. LAYOUT DO DASHBOARD 🔹
app.layout = html.Div(style={"display": "flex", "backgroundColor": "#1E3A5F"}, children=[
    html.Div(style={"width": "75%", "padding": "20px"}, children=[
        html.H2("FalaBR - Registros de Manifestações (Prefeitura de Florianópolis/SC)", style={"color": "white"}),
        dash_table.DataTable(
            id="tabela-manifestacoes",
            columns=[{"name": col, "id": col} for col in df.columns],
            data=df.to_dict("records"),
            page_size=45,
            sort_action="native",
            style_table={"overflowX": "auto"},
            style_cell={"textAlign": "left", "fontSize": "12px"},
            style_header={
                "backgroundColor": "#1E3A5F",
                "color": "white",
                "fontWeight": "bold",
                "border": "1px solid white",
            }
        ),
        html.Div(id="contador-registros", style={"margin-top": "10px", "font-weight": "bold", "font-size": "18px", "color": "white"}),
    ]),

    html.Div(style={"width": "25%", "padding": "20px", "border-left": "2px solid #ccc"}, children=[
        html.H3("Filtros", style={"color": "white", "fontSize": "15px"}),
        *[
            html.Div(style={"margin-bottom": "5px"}, children=[
                html.Label(f"{col}:", style={"color": "white", "fontSize": "10px"}),
                dcc.Dropdown(
                    id=f"filtro_{col}",
                    options=[{"label": str(val), "value": str(val)} for val in sorted(df[col].dropna().unique())] + [{"label": "Nenhum", "value": ""}],
                    multi=True,
                    style={"fontSize": "10px"}
                )
            ]) for col in filtros + filtros_adicionais
        ],

        html.Div(style={"margin-top": "20px", "display": "flex", "justify-content": "space-between"}, children=[
            html.Button("Aplicar Filtros", id="botao-aplicar-filtros", n_clicks=0, style={"background-color": "green", "color": "white"}),
            html.Button("Limpar Filtros", id="botao-limpar-filtros", n_clicks=0, style={"background-color": "red", "color": "white"})
        ]),

        html.Div(id="mensagem-botao", style={"margin-top": "10px", "font-weight": "bold", "color": "white"}),

        html.Div(style={"margin-top": "20px", "font-size": "12px", "color": "white"}, children=[
            html.P(["Fonte: ", html.A("CGU", href=url, target="_blank", style={"color": "#00BFFF", "textDecoration": "none"})]),
            html.P(f"Última atualização: {ultima_atualizacao}.")
        ])
    ])
])

# 🔹 8. CALLBACK PARA APLICAR E LIMPAR FILTROS 🔹
@app.callback(
    [Output("tabela-manifestacoes", "data"),
     Output("contador-registros", "children"),
     Output("mensagem-botao", "children")],
    [Input("botao-aplicar-filtros", "n_clicks"),
     Input("botao-limpar-filtros", "n_clicks")],
    [State(f"filtro_{col}", "value") for col in filtros + filtros_adicionais]
)
def atualizar_tabela(n_aplicar, n_limpar, *valores_filtros):
    df_filtrado = df.copy()
    ctx = dash.callback_context

    if ctx.triggered and "botao-limpar-filtros" in ctx.triggered[0]["prop_id"]:
        return df.to_dict("records"), f"Total de registros: {len(df):,.0f}".replace(",", "."), "Filtros resetados!"

    for col, valores in zip(filtros + filtros_adicionais, valores_filtros):
        if valores:
            df_filtrado = df_filtrado[df_filtrado[col].astype(str).isin(valores)]

    return df_filtrado.to_dict("records"), f"Total filtrado: {len(df_filtrado):,.0f}".replace(",", "."), "Filtros aplicados!"

import os

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))  # Usa a porta do ambiente ou 8080
    app.run_server(debug=True, host="0.0.0.0", port=port)
