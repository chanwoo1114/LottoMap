// 인증 폼 공용 검증 유틸

export const isValidEmail = (email: string) =>
  /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);

// 8~72자, 영문과 숫자 포함
export const isValidPassword = (password: string) =>
  password.length >= 8 &&
  password.length <= 72 &&
  /[A-Za-z]/.test(password) &&
  /[0-9]/.test(password);
