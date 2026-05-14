// Shared data + design tokens for 복권지도

const THEMES = {
  teal: {
    name: '바다 네잎', tagline: '틸 + 앰버 · 신뢰감',
    bg: '#EAF4F3', surface: '#FFFFFF',
    ink: '#0F2E33', inkSoft: '#5C7B80',
    primary: '#0E9488', primarySoft: '#C2E5E0',
    accent: '#F5B93D', mint: '#7FC6A8', lavender: '#8FB8D4',
    pink: '#F5B93D', hot: '#E8743C', cool: '#3B8CB8',
  },
  navy: {
    name: '황금 밤하늘', tagline: '네이비 + 골드 · 프리미엄',
    bg: '#F2F0E8', surface: '#FFFFFF',
    ink: '#14203D', inkSoft: '#6A7280',
    primary: '#1E3A6F', primarySoft: '#CFD7E5',
    accent: '#E0A93C', mint: '#8CC4A8', lavender: '#7E8EC2',
    pink: '#E0A93C', hot: '#C94A3E', cool: '#4A6DA8',
  },
  forest: {
    name: '숲속 행운', tagline: '딥그린 + 앰버 · 자연',
    bg: '#EFEEE4', surface: '#FFFFFF',
    ink: '#1F2B1F', inkSoft: '#6A7566',
    primary: '#3B6E3F', primarySoft: '#CEDAC5',
    accent: '#D89838', mint: '#7FAE7B', lavender: '#9AA8B8',
    pink: '#D89838', hot: '#C9612E', cool: '#5A8AA0',
  },
  cobalt: {
    name: '코발트 럭키', tagline: '블루 + 시트러스 · 경쾌',
    bg: '#EEF2F8', surface: '#FFFFFF',
    ink: '#15213D', inkSoft: '#5C6E85',
    primary: '#2E5BD6', primarySoft: '#CDD8EE',
    accent: '#F2C94C', mint: '#6BC2A6', lavender: '#8E9FC9',
    pink: '#F2C94C', hot: '#EE6B4A', cool: '#3D7FD9',
  },
  charcoal: {
    name: '차콜 앰버', tagline: '다크 뉴트럴 + 앰버 · 세련',
    bg: '#EEECE7', surface: '#FFFFFF',
    ink: '#1D1D1B', inkSoft: '#6D6A64',
    primary: '#2F2F2D', primarySoft: '#D4D1CB',
    accent: '#D99834', mint: '#8FAF91', lavender: '#9A9DA8',
    pink: '#D99834', hot: '#C85F37', cool: '#507486',
  },
  terracotta: {
    name: '테라코타', tagline: '따뜻한 어스톤 · 아날로그',
    bg: '#F3ECE2', surface: '#FFFFFF',
    ink: '#3A2218', inkSoft: '#8A6E5E',
    primary: '#B55437', primarySoft: '#E8CFBE',
    accent: '#E6B34A', mint: '#A8BF89', lavender: '#B8A594',
    pink: '#E6B34A', hot: '#B55437', cool: '#6A8BA0',
  },
};

// 동행복권 real game types
const GAMES = [
  { id: 'lotto', name: '로또 6/45', short: '로또', color: 'primary', emoji: '🎱' },
  { id: 'pension', name: '연금복권720+', short: '연금', color: 'mint', emoji: '💰' },
  { id: 'speetto', name: '스피또', short: '스피또', color: 'pink', emoji: '🎫' },
];

// Fake Seoul-area stores (pixel coords on a faux-map canvas 360x560)
// lucky = # of 1st place wins historically
const STORES = [
  { id: 's1', name: '행운편의점 강남점', addr: '서울 강남구 테헤란로 123', x: 198, y: 280, lucky: 7, rating: 4.8, reviews: 142, games: ['lotto','pension','speetto'], recent: '2025.11.08', distance: '120m' },
  { id: 's2', name: '대박로또판매점', addr: '서울 강남구 역삼로 45', x: 168, y: 252, lucky: 12, rating: 4.9, reviews: 298, games: ['lotto','pension'], recent: '2026.02.14', distance: '340m' },
  { id: 's3', name: '복돼지마트', addr: '서울 서초구 서초대로 88', x: 142, y: 310, lucky: 3, rating: 4.5, reviews: 67, games: ['lotto','speetto'], recent: '2024.07.21', distance: '680m' },
  { id: 's4', name: '황금복권방', addr: '서울 강남구 봉은사로 210', x: 232, y: 232, lucky: 9, rating: 4.7, reviews: 211, games: ['lotto','pension','speetto'], recent: '2026.01.03', distance: '520m' },
  { id: 's5', name: '신사꿈로또', addr: '서울 강남구 도산대로 55', x: 204, y: 198, lucky: 5, rating: 4.6, reviews: 88, games: ['lotto','pension'], recent: '2025.09.12', distance: '890m' },
  { id: 's6', name: '금요일마트', addr: '서울 서초구 강남대로 334', x: 110, y: 268, lucky: 4, rating: 4.4, reviews: 52, games: ['lotto','speetto'], recent: '2025.04.18', distance: '1.2km' },
  { id: 's7', name: '로또명당 삼성점', addr: '서울 강남구 영동대로 513', x: 258, y: 296, lucky: 15, rating: 4.9, reviews: 412, games: ['lotto','pension','speetto'], recent: '2026.03.21', distance: '1.5km', hot: true },
  { id: 's8', name: '럭키세븐 편의점', addr: '서울 강남구 선릉로 221', x: 220, y: 340, lucky: 2, rating: 4.3, reviews: 28, games: ['lotto'], recent: '2024.02.11', distance: '1.1km' },
  { id: 's9', name: '황제복권센터', addr: '서울 송파구 올림픽로 42', x: 295, y: 330, lucky: 11, rating: 4.8, reviews: 256, games: ['lotto','pension','speetto'], recent: '2025.12.27', distance: '2.3km', hot: true },
];

// Regional win stats for heatmap + ranking screens
const REGIONS = [
  { name: '강남구', wins: 48, pop: 540000, rank: 1, color: 'hot' },
  { name: '송파구', wins: 41, pop: 672000, rank: 2, color: 'hot' },
  { name: '서초구', wins: 33, pop: 410000, rank: 3, color: 'primary' },
  { name: '영등포구', wins: 28, pop: 398000, rank: 4, color: 'primary' },
  { name: '마포구', wins: 24, pop: 371000, rank: 5, color: 'accent' },
  { name: '종로구', wins: 19, pop: 154000, rank: 6, color: 'accent' },
  { name: '중구', wins: 15, pop: 123000, rank: 7, color: 'mint' },
  { name: '용산구', wins: 12, pop: 227000, rank: 8, color: 'mint' },
];

// AI-recommended sets (5 lines, 6 numbers each 1-45)
const AI_SETS = [
  { id: 'A', numbers: [3, 14, 17, 26, 38, 42], confidence: 87, basis: '최근 10회차 짝수 편중 패턴' },
  { id: 'B', numbers: [7, 11, 23, 29, 34, 41], confidence: 82, basis: '소수 집중 출현 구간 감지' },
  { id: 'C', numbers: [5, 12, 19, 28, 33, 45], confidence: 79, basis: '끝수 합 평균 회귀 구간' },
  { id: 'D', numbers: [2, 16, 22, 31, 37, 44], confidence: 76, basis: '이월 번호 + 콜드넘버 믹스' },
  { id: 'E', numbers: [9, 18, 24, 27, 35, 40], confidence: 73, basis: '고빈도 7개 중 6개 조합' },
];

// Number frequency over last 52 draws (for pattern viz)
const FREQ = [
  {n:1,c:6},{n:2,c:9},{n:3,c:11},{n:4,c:5},{n:5,c:8},{n:6,c:7},{n:7,c:10},
  {n:8,c:6},{n:9,c:8},{n:10,c:4},{n:11,c:12},{n:12,c:9},{n:13,c:7},{n:14,c:11},
  {n:15,c:5},{n:16,c:8},{n:17,c:13},{n:18,c:10},{n:19,c:9},{n:20,c:6},{n:21,c:7},
  {n:22,c:8},{n:23,c:11},{n:24,c:9},{n:25,c:5},{n:26,c:12},{n:27,c:10},{n:28,c:8},
  {n:29,c:11},{n:30,c:6},{n:31,c:9},{n:32,c:7},{n:33,c:10},{n:34,c:12},{n:35,c:9},
  {n:36,c:6},{n:37,c:8},{n:38,c:13},{n:39,c:5},{n:40,c:9},{n:41,c:11},{n:42,c:14},
  {n:43,c:7},{n:44,c:9},{n:45,c:6}
];

// My saved numbers
const MY_SAVED = [
  { id: 'm1', numbers: [3, 14, 17, 26, 38, 42], source: 'AI 추천 A', date: '2026.04.19', round: 1213, status: 'pending' },
  { id: 'm2', numbers: [7, 11, 23, 29, 34, 41], source: 'AI 추천 B', date: '2026.04.19', round: 1213, status: 'pending' },
  { id: 'm3', numbers: [1, 8, 15, 22, 33, 45], source: '직접 입력', date: '2026.04.12', round: 1212, status: 'lost', matched: 1 },
  { id: 'm4', numbers: [5, 12, 19, 26, 33, 44], source: '행운편의점 강남점', date: '2026.04.05', round: 1211, status: 'won5th', matched: 3, prize: 5000 },
];

// Number color by range (matches 동행복권 palette loosely)
const numColor = (n) => {
  if (n <= 10) return '#FFC759';  // yellow
  if (n <= 20) return '#5FB8E8';  // blue
  if (n <= 30) return '#FF7A7A';  // red
  if (n <= 40) return '#9BD4B7';  // green (mint-ish for our palette)
  return '#C9A8E8';               // purple
};

// Recent winning draws (latest first)
const WINNING_DRAWS = [
  { round: 1212, date: '2026.04.18', numbers: [3,14,22,26,33,42], bonus: 7,
    prize1: '23억 4,820만', winners1: 12, totalSales: '1,082억' },
  { round: 1211, date: '2026.04.11', numbers: [5,12,19,26,33,44], bonus: 21,
    prize1: '19억 8,210만', winners1: 14, totalSales: '1,065억' },
  { round: 1210, date: '2026.04.04', numbers: [1,8,16,27,35,44], bonus: 11,
    prize1: '22억 4,812만', winners1: 13, totalSales: '1,094억' },
  { round: 1209, date: '2026.03.28', numbers: [4,11,23,28,37,45], bonus: 19,
    prize1: '27억 900만', winners1: 11, totalSales: '1,103억' },
  { round: 1208, date: '2026.03.21', numbers: [2,17,24,29,36,41], bonus: 6,
    prize1: '17억 5,400만', winners1: 16, totalSales: '1,051억' },
  { round: 1207, date: '2026.03.14', numbers: [9,15,21,30,38,43], bonus: 4,
    prize1: '20억 1,200만', winners1: 14, totalSales: '1,072억' },
  { round: 1206, date: '2026.03.07', numbers: [6,13,20,25,34,40], bonus: 18,
    prize1: '15억 8,900만', winners1: 18, totalSales: '1,038억' },
  { round: 1205, date: '2026.02.28', numbers: [10,18,22,31,37,42], bonus: 3,
    prize1: '24억 6,100만', winners1: 12, totalSales: '1,089억' },
];

Object.assign(window, { THEMES, GAMES, STORES, REGIONS, AI_SETS, FREQ, MY_SAVED, WINNING_DRAWS, numColor });
