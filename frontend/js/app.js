const { createApp, ref, onMounted, computed } = Vue;

createApp({
    setup() {
        const API_URL = 'http://127.0.0.1:8000/api';
        
        const currentTab = ref('list');
        const operadoras = ref([]);
        const loading = ref(false);
        const errorMsg = ref(''); 
        const page = ref(1);
        const limit = 10;
        const totalItems = ref(0);
        const searchQuery = ref('');
        
        const stats = ref({ total_geral: 0, media_lancamento: 0, top_5_operadoras: [] });
        let chartInstance = null;

        const selectedOperadora = ref(null);
        const operadoraDespesas = ref([]);
        const loadingDetails = ref(false);

        const totalPages = computed(() => Math.ceil(totalItems.value / limit));

        const fetchOperadoras = async (p = 1) => {
            loading.value = true;
            errorMsg.value = ''; 
            
            try {
                const res = await axios.get(`${API_URL}/operadoras`, {
                    params: { page: p, limit, search: searchQuery.value }
                });
                
                operadoras.value = res.data.data;
                totalItems.value = res.data.total;
                page.value = res.data.page;

            } catch (e) { 
                console.error(e);
                errorMsg.value = 'Não foi possível conectar ao servidor. Verifique se a API Python está rodando.';
                operadoras.value = []; 
                totalItems.value = 0;
            } finally {
                loading.value = false; 
            }
        };

        const limparBusca = () => {
            searchQuery.value = ''; 
            fetchOperadoras(1);     
        };


        const loadStats = async () => {
            currentTab.value = 'stats';
            errorMsg.value = '';
            try {
                const res = await axios.get(`${API_URL}/estatisticas`);
                stats.value = res.data;
                renderChart(res.data.distribuicao_uf);
            } catch (e) { 
                console.error(e);
                errorMsg.value = 'Erro ao carregar estatísticas. Tente novamente mais tarde.';
            }
        };

        const openDetails = async (op) => {
            selectedOperadora.value = op;
            loadingDetails.value = true;
            try {
                const res = await axios.get(`${API_URL}/operadoras/${op.cnpj}/despesas`);
                operadoraDespesas.value = res.data;
            } catch (e) { 
                operadoraDespesas.value = []; 
            }
            loadingDetails.value = false;
        };

        const renderChart = (data) => {
            setTimeout(() => {
                const ctx = document.getElementById('chartUF');
                if (!ctx) return;
                if (chartInstance) chartInstance.destroy();
                
                chartInstance = new Chart(ctx, {
                    type: 'bar',
                    data: {
                        labels: data.map(d => d.uf),
                        datasets: [{
                            label: 'Total Despesas por UF',
                            data: data.map(d => d.total),
                            backgroundColor: '#3b82f6'
                        }]
                    }
                });
            }, 100);
        };


        const tabClass = (tab) => `px-4 py-2 font-medium rounded ${currentTab.value === tab ? 'bg-blue-600 text-white' : 'bg-white text-gray-700 hover:bg-gray-50'}`;
        const formatMoney = (val) => val ? val.toLocaleString('pt-BR', { minimumFractionDigits: 2 }) : '0,00';
        const formatDate = (dateStr) => {
            if(!dateStr) return '-';
            const [y, m, d] = dateStr.split(' ')[0].split('-');
            return `${d}/${m}/${y}`;
        }

        onMounted(() => fetchOperadoras());

        return { 
            currentTab, operadoras, loading, errorMsg, page, totalItems, totalPages, searchQuery,
            stats, selectedOperadora, operadoraDespesas, loadingDetails,
            fetchOperadoras, limparBusca, loadStats, openDetails, tabClass, formatMoney, formatDate 
        };
    }
}).mount('#app');