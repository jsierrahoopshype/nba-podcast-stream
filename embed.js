(function() {
    const SHEET_CSV_URL = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vRr8GiFRfKLquD0j49JqKIL11tnulG7--WeJsvP3nqFc72EeQM3RsQjnWeXlgcVoR2RL5y7oqYbfpSs/pub?gid=162816128&single=true&output=csv';
    const ITEMS_PER_LOAD = 15;
    const TRENDING_HOURS = 36;
    
    const NBA_PLAYERS = {
        'LeBron James': 'LeBron James', 'LeBron': 'LeBron James',
        'Stephen Curry': 'Stephen Curry', 'Steph Curry': 'Stephen Curry',
        'Kevin Durant': 'Kevin Durant', 'KD': 'Kevin Durant',
        'Giannis Antetokounmpo': 'Giannis Antetokounmpo', 'Giannis': 'Giannis Antetokounmpo',
        'Nikola Jokic': 'Nikola Jokic', 'Jokic': 'Nikola Jokic',
        'Joel Embiid': 'Joel Embiid', 'Embiid': 'Joel Embiid',
        'Jayson Tatum': 'Jayson Tatum', 'Luka Doncic': 'Luka Doncic', 'Luka': 'Luka Doncic',
        'James Harden': 'James Harden', 'Devin Booker': 'Devin Booker',
        'Anthony Davis': 'Anthony Davis', 'AD': 'Anthony Davis',
        'Victor Wembanyama': 'Victor Wembanyama', 'Wembanyama': 'Victor Wembanyama', 'Wemby': 'Victor Wembanyama',
        'Michael Jordan': 'Michael Jordan', 'MJ': 'Michael Jordan',
        'Kobe Bryant': 'Kobe Bryant', 'Kobe': 'Kobe Bryant',
        'Shaquille O\'Neal': 'Shaquille O\'Neal', 'Shaq': 'Shaquille O\'Neal'
    };
    
    const NBA_TEAMS = ['Lakers', 'Warriors', 'Celtics', 'Heat', 'Bucks', 'Nuggets', 'Suns', 'Mavericks', 'Sixers', '76ers', 'Knicks', 'Clippers', 'Kings'];
    const TOPICS = ['Trade Rumors', 'Trade Deadline', 'Free Agency', 'Playoffs', 'Play-In', 'NBA Finals', 'Draft', 'Draft Lottery', 'Rookie', 'MVP Race', 'All-Star', 'DPOY', 'Injury Report', 'Comeback', 'Suspension', 'Beef', 'Controversy', 'Drama', 'Game Winner', 'Clutch', 'Overtime', 'Record Breaking', 'Historic', 'Milestone', 'Coaching', 'Front Office', 'Ownership', 'In-Season Tournament', 'NBA Cup', 'Team USA'];
    
    let allVideos = [], filteredVideos = [], displayedCount = ITEMS_PER_LOAD;
    let currentFilter = 'all', currentSort = 'date', searchQuery = '', isLoading = false;
    let channelFilter = null;
    
    function createSlug(text) {
        return text.toLowerCase().replace(/[^a-z0-9\s-]/g, '').replace(/\s+/g, '-').replace(/-+/g, '-').trim();
    }
    
    function updateURL(type, value) {
        let hash = '';
        if (type === 'player') hash = `#player-${createSlug(value)}`;
        else if (type === 'topic') hash = `#topic-${createSlug(value)}`;
        else if (type === 'channel') hash = `#channel-${createSlug(value)}`;
        else if (type === 'sort') hash = `#sort-${createSlug(value)}`;
        else if (type === 'search') hash = `#search-${createSlug(value)}`;
        else if (type === 'filter') hash = `#filter-${createSlug(value)}`;
        window.history.pushState(null, '', hash || window.location.pathname);
    }
    
    function parseURL() {
        const hash = window.location.hash.substring(1);
        if (!hash) return { type: null, value: null };
        if (hash.startsWith('player-')) return { type: 'player', value: hash.substring(7) };
        if (hash.startsWith('topic-')) return { type: 'topic', value: hash.substring(6) };
        if (hash.startsWith('channel-')) return { type: 'channel', value: hash.substring(8) };
        if (hash.startsWith('sort-')) return { type: 'sort', value: hash.substring(5) };
        if (hash.startsWith('search-')) return { type: 'search', value: hash.substring(7) };
        if (hash.startsWith('filter-')) return { type: 'filter', value: hash.substring(7) };
        return { type: null, value: null };
    }
    
    function applyURLState() {
        const state = parseURL();
        if (!state.type) return;
        const readable = state.value.replace(/-/g, ' ');
        
        if (state.type === 'player' || state.type === 'topic') {
            searchQuery = readable;
            document.getElementById('nbaSearchBar').value = readable;
            filterVideos();
        } else if (state.type === 'channel') {
            const channel = allVideos.find(v => createSlug(v.channelName) === state.value);
            if (channel) { channelFilter = channel.channelName; filterVideos(); }
        } else if (state.type === 'sort') {
            currentSort = state.value;
            document.querySelectorAll('.nba-sort-btn').forEach(btn => {
                btn.classList.toggle('active', createSlug(btn.dataset.sort) === state.value);
            });
            filterVideos();
        } else if (state.type === 'search') {
            searchQuery = readable;
            document.getElementById('nbaSearchBar').value = readable;
            filterVideos();
        } else if (state.type === 'filter') {
            currentFilter = state.value;
            document.querySelectorAll('.nba-filter-btn').forEach(btn => {
                btn.classList.toggle('active', createSlug(btn.dataset.filter) === state.value);
            });
            filterVideos();
        }
    }
    
    function formatNumber(num) {
        const n = parseInt(num);
        if (isNaN(n)) return num;
        if (n >= 1000000) return (n / 1000000).toFixed(1) + 'M';
        if (n >= 1000) return (n / 1000).toFixed(1) + 'K';
        return n.toLocaleString();
    }
    
    function formatDate(dateString) {
        try {
            const date = new Date(dateString);
            if (isNaN(date.getTime())) return 'Unknown';
            return date.toLocaleString('en-US', { month: 'short', day: 'numeric', year: 'numeric', hour: 'numeric', minute: '2-digit', hour12: true });
        } catch { return 'Unknown'; }
    }
    
    function parseDuration(duration) {
        if (!duration) return 0;
        const parts = duration.split(':').map(Number);
        if (parts.length === 3) {
            if (parts[0] > 23) return parts[0] * 60 + parts[1];
            else return parts[0] * 3600 + parts[1] * 60 + parts[2];
        }
        if (parts.length === 2) return parts[0] * 60 + parts[1];
        if (parts.length === 1) return parts[0];
        return 0;
    }
    
    function calculateEngagementRate(video) {
        const views = parseInt(video.viewCount) || 1;
        const likes = parseInt(video.likeCount) || 0;
        return (likes / views) * 1000;
    }
    
    function calculateDiscussionRate(video) {
        const views = parseInt(video.viewCount) || 1;
        const comments = parseInt(video.commentCount) || 0;
        return (comments / views) * 1000;
    }
    
    function getAgeInHours(dateString) {
        try { return (new Date() - new Date(dateString)) / (1000 * 60 * 60); } catch { return 999; }
    }
    
    function isNew(dateString) { return getAgeInHours(dateString) < 24; }
    function isTrending(viewCount) { return parseInt(viewCount) > 50000; }
    function isRecent36Hours(dateString) { return getAgeInHours(dateString) <= 36; }
    
    function extractTopics(text) {
        if (!text) return [];
        return TOPICS.filter(t => text.toLowerCase().includes(t.toLowerCase())).slice(0, 5);
    }
    
    function detectPlayers(text) {
        if (!text) return {};
        const mentions = {};
        Object.entries(NBA_PLAYERS).forEach(([key, fullName]) => {
            const regex = new RegExp('\\b' + key.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '\\b', 'gi');
            const matches = text.match(regex);
            if (matches) mentions[fullName] = (mentions[fullName] || 0) + matches.length;
        });
        return mentions;
    }
    
    function detectTeams(text) {
        if (!text) return [];
        return [...new Set(NBA_TEAMS.filter(team => text.toLowerCase().includes(team.toLowerCase())))];
    }
    
    function calculateTrending() {
        const cutoffTime = new Date();
        cutoffTime.setHours(cutoffTime.getHours() - TRENDING_HOURS);
        const recentVideos = allVideos.filter(v => new Date(v.publishedDate) >= cutoffTime);
        const playerCounts = {}, teamCounts = {}, topicCounts = {};
        
        recentVideos.forEach(video => {
            const fullText = `${video.title} ${video.description}`;
            Object.keys(detectPlayers(fullText)).forEach(player => { playerCounts[player] = (playerCounts[player] || 0) + 1; });
            detectTeams(fullText).forEach(team => { teamCounts[team] = (teamCounts[team] || 0) + 1; });
            extractTopics(fullText).forEach(topic => { topicCounts[topic] = (topicCounts[topic] || 0) + 1; });
        });
        
        return {
            topPlayers: Object.entries(playerCounts).sort((a, b) => b[1] - a[1]).slice(0, 5),
            topTeams: Object.entries(teamCounts).sort((a, b) => b[1] - a[1]).slice(0, 5),
            topTopics: Object.entries(topicCounts).sort((a, b) => b[1] - a[1]).slice(0, 5)
        };
    }
    
    function renderTrending() {
        const { topPlayers, topTeams, topTopics } = calculateTrending();
        if (topPlayers.length === 0 && topTeams.length === 0 && topTopics.length === 0) {
            document.getElementById('nbaTrendingSection').style.display = 'none';
            return;
        }
        
        document.getElementById('nbaTrendingSection').style.display = 'block';
        let html = '';
        
        if (topPlayers.length > 0) {
            html += '<div class="nba-trending-category"><h4>🏀 Most Mentioned Players</h4>';
            topPlayers.forEach(([name, count]) => {
                html += `<div class="nba-trending-item" onclick="window.nbaFilterByName('${name.replace(/'/g, "\\'")}', 'player')"><span class="nba-trending-name">${name}</span><span class="nba-trending-count">${count}</span></div>`;
            });
            html += '</div>';
        }
        
        if (topTeams.length > 0) {
            html += '<div class="nba-trending-category"><h4>🏆 Most Discussed Teams</h4>';
            topTeams.forEach(([name, count]) => {
                html += `<div class="nba-trending-item" onclick="window.nbaFilterByName('${name}', 'topic')"><span class="nba-trending-name">${name}</span><span class="nba-trending-count">${count}</span></div>`;
            });
            html += '</div>';
        }
        
        if (topTopics.length > 0) {
            html += '<div class="nba-trending-category"><h4>🔥 Hot Topics</h4>';
            topTopics.forEach(([name, count]) => {
                html += `<div class="nba-trending-item" onclick="window.nbaFilterByTopic('${name}')"><span class="nba-trending-name">#${name}</span><span class="nba-trending-count">${count}</span></div>`;
            });
            html += '</div>';
        }
        
        document.getElementById('nbaTrendingGrid').innerHTML = html;
    }
    
    function clearAllFilters() {
        searchQuery = '';
        channelFilter = null;
        currentFilter = 'all';
        currentSort = 'date';
        document.getElementById('nbaSearchBar').value = '';
        document.querySelectorAll('.nba-filter-btn').forEach(btn => btn.classList.toggle('active', btn.dataset.filter === 'all'));
        document.querySelectorAll('.nba-sort-btn').forEach(btn => btn.classList.toggle('active', btn.dataset.sort === 'date'));
        window.history.pushState(null, '', window.location.pathname);
        filterVideos();
    }
    
    function filterByChannel(channelName) {
        channelFilter = channelFilter === channelName ? null : channelName;
        if (channelFilter) updateURL('channel', channelName);
        else window.history.pushState(null, '', window.location.pathname);
        filterVideos();
    }
    
    function filterByName(name, type = 'player') {
        searchQuery = name;
        document.getElementById('nbaSearchBar').value = name;
        channelFilter = null;
        updateURL(type, name);
        filterVideos();
    }
    
    function filterByTopic(topic) {
        searchQuery = topic;
        document.getElementById('nbaSearchBar').value = topic;
        channelFilter = null;
        updateURL('topic', topic);
        filterVideos();
    }
    
    window.nbaFilterByChannel = filterByChannel;
    window.nbaFilterByName = filterByName;
    window.nbaFilterByTopic = filterByTopic;
    
    function createVideoCard(video, index) {
        const fullText = `${video.title} ${video.description}`;
        const topics = extractTopics(fullText);
        const playerMentions = detectPlayers(fullText);
        const teams = detectTeams(fullText);
        const topPlayers = Object.entries(playerMentions).sort((a, b) => b[1] - a[1]).slice(0, 3);
        
        const card = document.createElement('div');
        card.className = 'nba-video-card';
        
        let statsHtml = `<div class="nba-stat-item">👁️ <strong>${formatNumber(video.viewCount)}</strong></div>`;
        if (video.likeCount && video.likeCount !== '0') statsHtml += `<div class="nba-stat-item">👍 <strong>${formatNumber(video.likeCount)}</strong></div>`;
        if (video.commentCount && video.commentCount !== '0') statsHtml += `<div class="nba-stat-item">💬 <strong>${formatNumber(video.commentCount)}</strong></div>`;
        
        const durationDisplay = video.duration ? `<span class="nba-duration-display">⏱️ ${video.duration}</span>` : '';
        const durationBadge = video.duration ? `<div class="nba-duration-badge">${video.duration}</div>` : '';
        
        card.innerHTML = `
            ${isNew(video.publishedDate) ? '<div class="nba-badge nba-new-badge">NEW</div>' : ''}
            ${isTrending(video.viewCount) ? '<div class="nba-badge nba-trending-badge">🔥 TRENDING</div>' : ''}
            <div class="nba-video-thumbnail">
                <img src="${video.thumbnail}" alt="${video.title.replace(/"/g, '&quot;')}">
                ${durationBadge}
            </div>
            <div class="nba-video-content">
                <h3 class="nba-video-title">
                    <a href="https://www.youtube.com/watch?v=${video.id}" target="_blank">${video.title}</a>
                </h3>
                <div class="nba-video-meta">
                    <span class="nba-channel-name" onclick="window.nbaFilterByChannel('${video.channelName.replace(/'/g, "\\'")}')">${video.channelName}</span>
                    <span>📅 ${formatDate(video.publishedDate)}</span>
                    ${durationDisplay}
                </div>
                <div class="nba-stats-row">${statsHtml}</div>
                ${topics.length > 0 ? `<div class="nba-topic-tags">${topics.map(t => `<span class="nba-topic-tag" onclick="window.nbaFilterByTopic('${t}')">#${t}</span>`).join('')}</div>` : ''}
                ${topPlayers.length > 0 ? `<div class="nba-player-mentions">🏀 ${topPlayers.map(([name, count]) => `<span class="nba-clickable-name" onclick="window.nbaFilterByName('${name.replace(/'/g, "\\'")}')">${name}</span> (${count})`).join(', ')}</div>` : ''}
                ${teams.length > 0 ? `<div class="nba-team-mentions">🏆 ${teams.slice(0, 3).map(team => `<span class="nba-clickable-name" onclick="window.nbaFilterByName('${team}')">${team}</span>`).join(', ')}</div>` : ''}
            </div>
        `;
        
        return card;
    }
    
    function filterVideos() {
        let result = allVideos;
        if (channelFilter) result = result.filter(v => v.channelName === channelFilter);
        
        if (searchQuery) {
            const query = searchQuery.toLowerCase();
            result = result.filter(v => {
                const fullText = `${v.title} ${v.channelName} ${v.description}`.toLowerCase();
                const topics = extractTopics(fullText).join(' ').toLowerCase();
                const playerNames = Object.keys(detectPlayers(fullText)).join(' ').toLowerCase();
                const teamNames = detectTeams(fullText).join(' ').toLowerCase();
                return fullText.includes(query) || topics.includes(query) || playerNames.includes(query) || teamNames.includes(query);
            });
            result.sort((a, b) => new Date(b.publishedDate) - new Date(a.publishedDate));
            filteredVideos = result;
            displayedCount = ITEMS_PER_LOAD;
            renderVideos();
            return;
        }
        
        switch(currentFilter) {
            case 'trending': result = result.filter(v => isTrending(v.viewCount) && isRecent36Hours(v.publishedDate)); break;
            case 'new': result = result.filter(v => isNew(v.publishedDate)); break;
        }
        
        switch(currentSort) {
            case 'date': result.sort((a, b) => new Date(b.publishedDate) - new Date(a.publishedDate)); break;
            case 'views': result = result.filter(v => isRecent36Hours(v.publishedDate)); result.sort((a, b) => parseInt(b.viewCount) - parseInt(a.viewCount)); break;
            case 'hottest': result = result.filter(v => isRecent36Hours(v.publishedDate)); result.sort((a, b) => calculateEngagementRate(b) - calculateEngagementRate(a)); break;
            case 'discussed': result = result.filter(v => isRecent36Hours(v.publishedDate)); result.sort((a, b) => calculateDiscussionRate(b) - calculateDiscussionRate(a)); break;
            case 'likes': result = result.filter(v => isRecent36Hours(v.publishedDate)); result.sort((a, b) => parseInt(b.likeCount || 0) - parseInt(a.likeCount || 0)); break;
            case 'comments': result = result.filter(v => isRecent36Hours(v.publishedDate)); result.sort((a, b) => parseInt(b.commentCount || 0) - parseInt(a.commentCount || 0)); break;
            case 'duration': result = result.filter(v => isRecent36Hours(v.publishedDate)); result.sort((a, b) => parseDuration(b.duration) - parseDuration(a.duration)); break;
            case 'channel': result = result.filter(v => isRecent36Hours(v.publishedDate)); result.sort((a, b) => a.channelName.localeCompare(b.channelName)); break;
        }
        
        filteredVideos = result;
        displayedCount = ITEMS_PER_LOAD;
        renderVideos();
    }
    
    function renderVideos() {
        const grid = document.getElementById('nbaVideoGrid');
        const videosToShow = filteredVideos.slice(0, displayedCount);
        
        if (videosToShow.length === 0) {
            grid.innerHTML = '<div class="nba-no-videos">No episodes found.</div>';
            return;
        }
        
        grid.innerHTML = '';
        videosToShow.forEach((video, i) => {
            const card = createVideoCard(video, i);
            grid.appendChild(card);
        });
    }
    
    function handleScroll() {
        if (isLoading) return;
        const scrollPos = window.innerHeight + window.scrollY;
        const pageHeight = document.documentElement.scrollHeight;
        
        if (scrollPos >= pageHeight - 500 && displayedCount < filteredVideos.length) {
            isLoading = true;
            setTimeout(() => {
                displayedCount += ITEMS_PER_LOAD;
                renderVideos();
                isLoading = false;
            }, 500);
        }
    }
    
    async function loadVideos() {
        try {
            const response = await fetch(SHEET_CSV_URL);
            if (!response.ok) throw new Error('Failed to fetch');
            
            const csvText = await response.text();
            const parsed = Papa.parse(csvText, { header: true, skipEmptyLines: true });
            
            if (!parsed.data || parsed.data.length === 0) {
                document.getElementById('nbaVideoGrid').innerHTML = '<div class="nba-no-videos">No videos yet.</div>';
                return;
            }
            
            allVideos = parsed.data.map(row => ({
                id: row['Video ID'],
                title: row['Title'],
                channelName: row['Channel Name'],
                channelId: row['Channel ID'],
                publishedDate: row['Published Date'],
                thumbnail: row['Thumbnail URL'],
                description: row['Description'] || '',
                viewCount: row['View Count'] || '0',
                duration: row['Duration'] || '',
                likeCount: row['Like Count'] || '0',
                commentCount: row['Comment Count'] || '0',
            })).filter(v => v.id);
            
            renderTrending();
            applyURLState();
            if (!window.location.hash) filterVideos();
        } catch (error) {
            document.getElementById('nbaVideoGrid').innerHTML = `<div class="nba-error">Unable to load: ${error.message}</div>`;
        }
    }
    
    // Create HTML structure
    const container = document.getElementById('nba-podcast-embed');
    container.innerHTML = `
        <div class="nba-podcast-container">
            <div class="nba-header">
                <h1>🏀 NBA Podcast Stream</h1>
                <p>Latest episodes from 57 NBA podcasts</p>
            </div>
            <div class="nba-controls">
                <div class="nba-search-row">
                    <input type="text" class="nba-search-bar" id="nbaSearchBar" placeholder="🔍 Search title, channel, description, topics, players, teams...">
                    <button class="nba-clear-btn" id="nbaClearBtn">✕ Clear All</button>
                </div>
                <div class="nba-filters">
                    <button class="nba-filter-btn active" data-filter="all">All Episodes</button>
                    <button class="nba-filter-btn" data-filter="trending">🔥 Trending</button>
                    <button class="nba-filter-btn" data-filter="new">🆕 New Today</button>
                </div>
                <div class="nba-sort-controls">
                    <span class="nba-sort-label">Sort:</span>
                    <button class="nba-sort-btn active" data-sort="date">Latest</button>
                    <button class="nba-sort-btn" data-sort="views">Most Viewed</button>
                    <button class="nba-sort-btn" data-sort="hottest">⭐ Best Rated</button>
                    <button class="nba-sort-btn" data-sort="discussed">💬 Most Discussed</button>
                    <button class="nba-sort-btn" data-sort="likes">Most Liked</button>
                    <button class="nba-sort-btn" data-sort="comments">Most Comments</button>
                    <button class="nba-sort-btn" data-sort="duration">Duration</button>
                    <button class="nba-sort-btn" data-sort="channel">Channel A-Z</button>
                </div>
            </div>
            <div class="nba-trending-section" id="nbaTrendingSection" style="display:none">
                <h3>🔥 Trending in Last 36 Hours</h3>
                <div class="nba-trending-grid" id="nbaTrendingGrid"></div>
            </div>
            <div id="nbaVideoGrid" class="nba-video-grid">
                <div class="nba-loading">Loading episodes...</div>
            </div>
        </div>
    `;
    
    // Load PapaParse if not already loaded
    if (typeof Papa === 'undefined') {
        const script = document.createElement('script');
        script.src = 'https://cdnjs.cloudflare.com/ajax/libs/PapaParse/5.3.2/papaparse.min.js';
        script.onload = () => {
            initializeApp();
        };
        document.head.appendChild(script);
    } else {
        initializeApp();
    }
    
    function initializeApp() {
        document.getElementById('nbaClearBtn').addEventListener('click', clearAllFilters);
        
        let searchTimeout;
        document.getElementById('nbaSearchBar').addEventListener('input', (e) => {
            clearTimeout(searchTimeout);
            searchTimeout = setTimeout(() => {
                searchQuery = e.target.value;
                channelFilter = null;
                if (searchQuery) updateURL('search', searchQuery);
                else window.history.pushState(null, '', window.location.pathname);
                filterVideos();
            }, 300);
        });
        
        document.querySelectorAll('.nba-filter-btn').forEach(btn => {
            btn.addEventListener('click', () => {
                document.querySelectorAll('.nba-filter-btn').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                currentFilter = btn.dataset.filter;
                updateURL('filter', currentFilter);
                filterVideos();
            });
        });
        
        document.querySelectorAll('.nba-sort-btn').forEach(btn => {
            btn.addEventListener('click', () => {
                document.querySelectorAll('.nba-sort-btn').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                currentSort = btn.dataset.sort;
                updateURL('sort', currentSort);
                filterVideos();
            });
        });
        
        window.addEventListener('scroll', handleScroll);
        window.addEventListener('popstate', applyURLState);
        
        loadVideos();
        setInterval(loadVideos, 60 * 60 * 1000);
    }
})();
