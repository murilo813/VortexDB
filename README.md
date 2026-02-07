🌪️ VortexDB
VortexDB é uma engine de banco de dados experimental focada em latência ultra-baixa e eficiência de IO. Nascido da frustração com abstrações pesadas, o Vortex foi projetado para ser o "vórtice" onde os dados entram e saem na velocidade do metal.

🛠️ Early Development (Prototipagem em Python / Migrando para Rust)

🚀 O Diferencial
O VortexDB não tenta ser um canivete suíço. Ele foca em:

Storage Engine Customizada: Nada de arquivos de texto. O Vortex manipula bytes diretamente.

In-Memory First: Otimizado para operar em memória com persistência assíncrona.

Performance-Driven: Evoluindo de um protótipo funcional em Python para uma engine de alto desempenho escrita em Rust.

🏗️ Arquitetura (Roadmap)
Phase 1 (Python): Validação de algoritmos, estruturas de dados (Linked Lists, B-Trees) e lógica de indexação.

Phase 2 (Rust - Current): Reescrita do core para gerenciamento manual de memória, Zero-copy e Safety concurrency.

Phase 3 (Network): Implementação de um protocolo binário próprio para comunicação cliente-servidor.

🤝 Contribuição
Se você não tem medo de ponteiros, locks e gerenciamento de memória no braço, sinta-se em casa.

Projeto feito principalmente para o aprendizado de funcionamento de banco de dados relacionais, mas com pretenção de se tornar uma opção em projetos.
