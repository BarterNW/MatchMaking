// Global state
let sponsors = [];

// Initialize on page load
document.addEventListener('DOMContentLoaded', async () => {
    await loadSponsors();
    setupEventListeners();
});

// Load sponsors from API
async function loadSponsors() {
    try {
        const response = await fetch('/api/sponsors');
        const data = await response.json();
        sponsors = data.sponsors;
        
        const select = document.getElementById('sponsorSelect');
        sponsors.forEach(sponsor => {
            const option = document.createElement('option');
            option.value = sponsor.id;
            option.textContent = `${sponsor.sponsor_name} (${sponsor.status})`;
            select.appendChild(option);
        });
    } catch (error) {
        console.error('Error loading sponsors:', error);
        alert('Failed to load sponsors. Please refresh the page.');
    }
}

// Setup event listeners
function setupEventListeners() {
    const select = document.getElementById('sponsorSelect');
    const button = document.getElementById('findMatchesBtn');
    
    select.addEventListener('change', () => {
        button.disabled = !select.value;
    });
    
    button.addEventListener('click', async () => {
        const sponsorId = parseInt(select.value);
        if (sponsorId) {
            await findMatches(sponsorId);
        }
    });
}

// Find matches for selected sponsor
async function findMatches(sponsorId) {
    const loading = document.getElementById('loading');
    const results = document.getElementById('results');
    const noResults = document.getElementById('noResults');
    const matchesContainer = document.getElementById('matchesContainer');
    const resultsTitle = document.getElementById('resultsTitle');
    
    // Show loading, hide results
    loading.classList.remove('hidden');
    results.classList.add('hidden');
    noResults.classList.add('hidden');
    matchesContainer.innerHTML = '';
    
    try {
        const response = await fetch(`/api/sponsors/${sponsorId}/matches`);
        const data = await response.json();
        
        loading.classList.add('hidden');
        
        if (data.matches && data.matches.length > 0) {
            resultsTitle.textContent = `Matches for ${data.sponsor_name}`;
            results.classList.remove('hidden');
            
            data.matches.forEach(match => {
                const card = createMatchCard(match);
                matchesContainer.appendChild(card);
            });
        } else {
            noResults.classList.remove('hidden');
        }
    } catch (error) {
        console.error('Error finding matches:', error);
        loading.classList.add('hidden');
        alert('Failed to find matches. Please try again.');
    }
}

// Create a match card element
function createMatchCard(match) {
    const card = document.createElement('div');
    card.className = 'match-card';
    
    const matchHeader = document.createElement('div');
    matchHeader.className = 'match-header';
    
    const eventName = document.createElement('div');
    eventName.className = 'event-name';
    eventName.textContent = match.event_name;
    
    const matchPercentage = document.createElement('div');
    matchPercentage.className = 'match-percentage';
    matchPercentage.textContent = `${match.match_percentage.toFixed(1)}%`;
    
    matchHeader.appendChild(eventName);
    matchHeader.appendChild(matchPercentage);
    
    const explanation = document.createElement('div');
    explanation.className = 'match-explanation';
    explanation.textContent = match.explanation;
    
    const breakdown = document.createElement('div');
    breakdown.className = 'breakdown';
    
    const breakdownTitle = document.createElement('div');
    breakdownTitle.className = 'breakdown-title';
    breakdownTitle.textContent = 'Score Breakdown:';
    breakdown.appendChild(breakdownTitle);
    
    // Add breakdown items
    const features = ['geography', 'budget', 'sponsorship_type', 'event_type', 'footfall'];
    features.forEach(feature => {
        if (match.breakdown[feature]) {
            const item = createBreakdownItem(feature, match.breakdown[feature]);
            breakdown.appendChild(item);
        }
    });
    
    card.appendChild(matchHeader);
    card.appendChild(explanation);
    card.appendChild(breakdown);
    
    return card;
}

// Create a breakdown item element
function createBreakdownItem(featureName, featureData) {
    const item = document.createElement('div');
    item.className = 'breakdown-item';
    
    // Determine match class
    if (featureData.match_factor === 1.0) {
        item.classList.add('full-match');
    } else if (featureData.match_factor === 0.5) {
        item.classList.add('partial-match');
    } else {
        item.classList.add('no-match');
    }
    
    const header = document.createElement('div');
    header.className = 'breakdown-item-header';
    
    const name = document.createElement('div');
    name.className = 'breakdown-item-name';
    name.textContent = featureName.replace('_', ' ');
    
    const score = document.createElement('div');
    score.className = 'breakdown-item-score';
    score.textContent = `${featureData.contribution.toFixed(1)} / ${featureData.weight.toFixed(1)} (${(featureData.match_factor * 100).toFixed(0)}% match)`;
    
    header.appendChild(name);
    header.appendChild(score);
    
    const explanation = document.createElement('div');
    explanation.className = 'breakdown-item-explanation';
    explanation.textContent = featureData.explanation;
    
    item.appendChild(header);
    item.appendChild(explanation);
    
    return item;
}
