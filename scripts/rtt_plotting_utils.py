import matplotlib.pyplot as plt
import numpy as np


def plot_swimlane_chart():
    # Define data
    locations = ['Dishy→GS\nGS→PoP (DE)', 'Dishy→GS\nGS→PoP (DE)', 'Fiber (DE→DE)', 'Fiber (RU→DE)']
    medians = [25, 25, 125, 150]
    spreads = [10, 10, 50, 40]  # This represents the spread of the data
    positions = [3, 2, 1, 0]  # Reverse order for bottom-to-top layout

    fig, ax = plt.subplots(figsize=(12, 6))

    # Create box plots
    for i, (median, spread, position) in enumerate(zip(medians, spreads, positions)):
        # Create synthetic data for the boxplot
        data = np.random.normal(median, spread / 2, 100)
        box = ax.boxplot(data, positions=[position], widths=0.6,
                         patch_artist=True, vert=False)

        # Color setting
        if i < 2:
            color = 'lightcoral'
        elif i == 2:
            color = 'khaki'
        else:
            color = 'lightskyblue'

        # Set colors
        plt.setp(box['boxes'], facecolor=color)
        plt.setp(box['medians'], color='black', linewidth=1.5)

        # Add text for median
        ax.text(median, position, f'{median}',
                ha='center', va='center', fontweight='bold',
                bbox=dict(facecolor='white', edgecolor='none', alpha=0.7))

    # Customize the plot
    ax.set_ylim(-0.5, 3.5)
    ax.set_xlim(0, 260)
    ax.set_yticks(positions)
    ax.set_yticklabels(locations)
    ax.set_xlabel('RTT [ms]')
    ax.set_title('Network Latency Sequence Diagram (Boxplot Version)')

    # Add vertical lines
    for x in [50, 100, 150, 200]:
        ax.axvline(x=x, color='gray', linestyle='--', alpha=0.5)

    # Add region labels
    ax.text(25, -0.25, 'Germany (DE)', ha='center', va='center', fontweight='bold')
    ax.text(200, 3.25, 'Réunion Island (RU)', ha='center', va='center', fontweight='bold')

    plt.tight_layout()
    plt.show()


if __name__ == '__main__':
    plot_swimlane_chart()
