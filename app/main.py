import streamlit as st
from processor import *

TEXT_LOANDING = "Carregando..."

def display_home():
    st.title("Blockbuster - Catologo")
    st.markdown("")
    with st.spinner(TEXT_LOANDING):
        df = get_all_titles()
        st.write(df.toPandas())

def display_amazon():
    st.subheader("Amazon Prime - Catalogo")
    st.markdown("")
    with st.spinner(TEXT_LOANDING):
        df = get_titles_by_source("amazon prime")
        st.write(df.toPandas())

def display_netflix():
    st.subheader("Netflix - Catalogo")
    st.markdown("")
    with st.spinner(TEXT_LOANDING):
        df = get_titles_by_source("netflix")
        st.write(df.toPandas())

def display_disney():
    st.subheader("Disney Plus - Catalogo")
    st.markdown("")
    with st.spinner(TEXT_LOANDING):
        df = get_titles_by_source("disney plus")
        st.write(df.toPandas())

def main():
    
    pages = {
        "Todas": display_home,
        "Amazon Prime": display_amazon,
        "Netflix": display_netflix,
        "Disney Plus": display_disney,
    }

    st.sidebar.header("Blockbuster")
    
    page = st.sidebar.selectbox("Selecione uma plataforma:", tuple(pages.keys()))
    pages[page]()

if __name__ == "__main__":
    main()