import streamlit as st
import duckdb
import os
import pandas as pd
import plotly.express as px

# Configuração da página para ocupar a tela toda
st.set_page_config(page_title="EduData | Raio-X IDEB", page_icon="📊", layout="wide")

# Injetando CSS personalizado para mudar o fundo e criar caixas de destaque
st.markdown("""
    <style>
    /* Cor de fundo cinza bem claro para o painel */
    .stApp {
        background-color: #f4f6f9;
    }
    /* Caixa de destaque estilo relatório executivo */
    .caixa-destaque {
        background-color: #ffffff;
        padding: 20px;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        border-left: 6px solid #2E86C1;
        margin-bottom: 20px;
    }
    /* Caixa de alerta para o plano de ação */
    .caixa-alerta {
        background-color: #fff3cd;
        padding: 20px;
        border-radius: 10px;
        border-left: 6px solid #ffc107;
        color: #856404;
        margin-bottom: 20px;
    }
    </style>
""", unsafe_allow_html=True)

CAMINHO_ATUAL = os.path.dirname(os.path.abspath(__file__))
CAMINHO_BASE = os.path.abspath(os.path.join(CAMINHO_ATUAL, "..", ".."))
CAMINHO_GOLD_RESUMO = os.path.join(CAMINHO_BASE, "data", "gold", "indicadores_municipais.parquet")
CAMINHO_GOLD_HISTORICO = os.path.join(CAMINHO_BASE, "data", "gold", "historico_ideb.parquet")

META_MEC = 6.0

@st.cache_data
def carregar_dados():
    df_resumo = duckdb.query(f"SELECT * FROM read_parquet('{CAMINHO_GOLD_RESUMO}')").df()
    df_historico = duckdb.query(f"SELECT * FROM read_parquet('{CAMINHO_GOLD_HISTORICO}')").df()
    df_historico['ano'] = df_historico['ano'].astype(str)
    return df_resumo, df_historico

def main():
    st.title("📊 Painel Executivo de Educação: Monitoramento IDEB")
    
    df_resumo, df_historico = carregar_dados()
    
    # Textos Dinâmicos baseados nos dados
    media_brasil = round(df_resumo['media_ideb'].mean(), 2)
    capitais_na_meta = df_resumo[df_resumo['media_ideb'] >= META_MEC]
    capitais_criticas = df_resumo[df_resumo['media_ideb'] < META_MEC].sort_values(by='media_ideb')
    melhor_capital = df_resumo.iloc[0]['nome_municipio']
    pior_capital = capitais_criticas.iloc[0]['nome_municipio']

    st.markdown(f"""
        <div class="caixa-destaque">
            <h4>Resumo Executivo</h4>
            <p>Este relatório analisa o desempenho de <b>{len(df_resumo)} capitais brasileiras</b>, processando mais de meio milhão de registros escolares históricos. 
            Atualmente, a média nacional está em <b>{media_brasil}</b>. A meta estabelecida pelo MEC é <b>{META_MEC}</b>. 
            O município com maior destaque atual é <b>{melhor_capital}</b>, enquanto <b>{pior_capital}</b> necessita de intervenção prioritária.</p>
        </div>
    """, unsafe_allow_html=True)
    
    # Criando ABAS para organizar o visual
    aba1, aba2 = st.tabs(["📈 Visão Geral em Gráficos", "📋 Diagnóstico e Plano de Ação"])
    
    with aba1:
        col1, col2, col3 = st.columns(3)
        with col1: st.metric("Média Geral (Brasil)", media_brasil, delta=round(media_brasil - META_MEC, 2))
        with col2: st.metric("Capitais na Meta (≥ 6.0)", len(capitais_na_meta))
        with col3: st.metric("Capitais em Alerta (< 6.0)", len(capitais_criticas))

        st.divider()
        
        # Gráficos em colunas para aproveitar a tela
        col_graf1, col_graf2 = st.columns(2)
        
        with col_graf1:
            st.subheader("Evolução Nacional")
            fig_linha = px.line(df_historico, x='ano', y='media_anual', markers=True)
            fig_linha.update_traces(line=dict(color="#3498DB", width=4), marker=dict(size=8))
            fig_linha.add_hline(y=META_MEC, line_dash="dash", line_color="green", annotation_text="Meta 6.0")
            st.plotly_chart(fig_linha, use_container_width=True)
            
        with col_graf2:
            st.subheader("Desempenho por Capital")
            df_resumo['Status'] = df_resumo['media_ideb'].apply(lambda x: 'Atingiu a Meta' if x >= META_MEC else 'Abaixo da Meta')
            fig_barras = px.bar(df_resumo, x='nome_municipio', y='media_ideb', color='Status',
                                color_discrete_map={'Atingiu a Meta': '#27AE60', 'Abaixo da Meta': '#E74C3C'})
            fig_barras.add_hline(y=META_MEC, line_dash="dash", line_color="black")
            st.plotly_chart(fig_barras, use_container_width=True)

    with aba2:
        st.markdown(f"""
            <div class="caixa-alerta">
                <h4>⚠️ Diagnóstico: Efeito Pandemia e Disparidade Regional</h4>
                <p>Os dados revelam uma queda acentuada na média nacional no ano de 2021, um reflexo direto do distanciamento social e fechamento das escolas. 
                Além disso, <b>{len(capitais_criticas)} capitais</b> ainda não conseguiram retomar ou atingir a meta de {META_MEC} pontos estipulada para a educação básica.</p>
            </div>
        """, unsafe_allow_html=True)
        
        st.subheader("🎯 Plano de Intervenção Estratégico (Próximos 12 meses)")
        
        col_acao1, col_acao2 = st.columns(2)
        
        with col_acao1:
            st.markdown("### 1. Ações Pedagógicas")
            st.markdown("- **Reforço no Contraturno:** Implementação imediata de tutoria em Matemática e Português para os municípios na zona vermelha.")
            st.markdown("- **Busca Ativa:** Combate à evasão escolar gerada pós-2021, utilizando cruzamento de dados de frequência.")
            st.markdown("- **Avaliações Diagnósticas Bimestrais:** Não esperar o próximo IDEB. Medir o progresso internamente a cada 2 meses.")

        with col_acao2:
            st.markdown("### 2. Ações de Gestão e Infraestrutura")
            st.markdown(f"- **Foco Triplicado em {pior_capital}:** Redirecionamento de 15% do orçamento de melhorias prioritárias para a capital com o índice mais crítico.")
            st.markdown("- **Programa de Valorização:** Bonificação atrelada a resultados para professores de escolas que saírem da zona de alerta.")
            st.markdown("- **Digitalização das Salas:** Expansão da banda larga para permitir o uso de plataformas de ensino adaptativo nas escolas públicas.")

if __name__ == "__main__":
    main()