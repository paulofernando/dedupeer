package deduplication.gui.component.renderer;

import java.awt.Color;
import java.awt.Component;
import java.awt.Graphics;

import javax.swing.JProgressBar;
import javax.swing.JTable;
import javax.swing.table.TableCellRenderer;

public class JProgressRenderer extends JProgressBar implements TableCellRenderer {

	private static final long serialVersionUID = -4116117091520144073L;
	private Color background, foreground;
	private ProgressInfo progressInfo;
	
	@Override
	public Component getTableCellRendererComponent(JTable table, Object value,
			boolean selected, boolean hasFocus, int row, int column) {
		
		progressInfo = (ProgressInfo) value;
		this.setValue(progressInfo.getProgress());
		
		if(selected) {
			background = table.getSelectionBackground();
		} else {
			background = table.getBackground();
		}
		invalidate();
		return this;
	}
	
	public void paint(Graphics graphics) {
		super.paint(graphics);
		if(this.getValue() == 0) {
			graphics.setColor(background);
			graphics.fillRect(0, 0, this.getWidth(), this.getHeight());
		} else {
			graphics.drawString(progressInfo.getTypeString(), (this.getWidth()>>1) - (graphics.getFontMetrics().stringWidth(progressInfo.getTypeString())), ((this.getHeight()>>1) - (graphics.getFontMetrics().getHeight()>>1)));
		}
	}
}
