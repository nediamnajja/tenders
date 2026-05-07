/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,jsx}'],
  theme: {
    extend: {
      colors: {
        kpmg: {
          blue:      '#00338D',
          lightblue: '#005EB8',
          cobalt:    '#0091DA',
          violet:    '#483698',
          teal:      '#00A3A1',
          green:     '#009A44',
          yellow:    '#EAAA00',
          red:       '#BC204B',
          gray:      '#63666A',
          lightgray: '#F5F5F5',
        },
      },
      fontFamily: {
        sans: ['Inter', 'system-ui', 'sans-serif'],
      },
    },
  },
  plugins: [],
}