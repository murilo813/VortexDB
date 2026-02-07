# 🌪️ VortexDB

**VortexDB** é uma engine de banco de dados experimental projetada para latência ultra-baixa e eficiência máxima de I/O. O projeto nasceu da frustração com abstrações pesadas de alto nível, com o objetivo de criar um "vórtice" onde os dados fluem na velocidade do metal.

> 🛠️ **Status:** Early Development (Prototipando em Python / Migrando para Rust)

---

## 🚀 O Diferencial
O VortexDB não tenta ser um canivete suíço. Ele é focado no que importa para performance:
* **Storage Engine Customizada:** Zero arquivos de texto. O Vortex manipula bytes diretamente no disco.
* **In-Memory First:** Arquitetura otimizada para operações em memória com persistência assíncrona.
* **Performance-Driven:** Evoluindo de um protótipo em Python para uma engine de alto desempenho escrita em **Rust**, focando em *Zero-copy* e *Lock-free concurrency*.

## 🏗️ Roadmap de Arquitetura
* [x] **Phase 1 (Python):** Validação de algoritmos, estruturas de dados e lógica de indexação.
* [ ] **Phase 2 (Rust - Current):** Reescrita do core, gerenciamento manual de memória e segurança de concorrência.
* [ ] **Phase 3 (Network):** Implementação de protocolo binário próprio para comunicação cliente-servidor.


## 📜 Especificação do Formato `.vortex`
Para garantir que o acesso ao disco seja previsível e atômico, o Vortex utiliza um layout de **Slotted Pages** com páginas de **4KB**.


## 🎯 Objetivo do Projeto
Este projeto foi iniciado principalmente para o estudo profundo de funcionamento de bancos de dados relacionais e sistemas de baixo nível. No entanto, a pretensão é evoluir o VortexDB para que se torne uma opção viável, rápida e leve para projetos reais que demandam controle total sobre a persistência de dados.

## 🤝 Contribuição
Se você gosta de escovar bits e não tem medo de ponteiros, *locks* e gerenciamento de memória no braço, sinta-se em casa para contribuir.

Projeto feito principalmente para o aprendizado de funcionamento de banco de dados relacionais, mas com pretenção de se tornar uma opção em projetos.
